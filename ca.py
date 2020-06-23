from spade import quit_spade
import time
import datetime
from spade.agent import Agent
from spade.behaviour import CyclicBehaviour, PeriodicBehaviour
from spade.template import Template
from spade.message import Message
import sys
import pandas as pd
import logging
import argparse
import operative_functions as opf
import os


# CA
class ContinuousAnnealingAgent(Agent):
    class CABehav(PeriodicBehaviour):
        async def run(self):
            global process_df, ca_status_var, my_full_name, ca_status_started_at, stop_time, my_dir, wait_msg_time, ca_data_df, plc_temp_df, auction_df, fab_started_at, leeway, op_times_df, auction_start, ca_to_tr_df
            """inform log of status"""
            print(f'op_times_df: {op_times_df}')
            ca_activation_json = opf.activation_df(my_full_name, ca_status_started_at, op_times_df)
            ca_msg_log = opf.msg_to_log(ca_activation_json, my_dir)
            await self.send(ca_msg_log)
            if ca_status_var == "pre-auction":
                pre_auction_start = datetime.datetime.now()
                auction_df.at[0, 'pre_auction_start'] = pre_auction_start  # save to auction df
                print(f'pre_auction_start: {pre_auction_start}')
                """inform log of status"""
                ca_inform_json = opf.inform_log_df(my_full_name, ca_status_started_at, ca_status_var).to_json()
                ca_msg_log = opf.msg_to_log(ca_inform_json, my_dir)
                await self.send(ca_msg_log)
                """Asks browser agent for ca_op_time and tr_op_time and locations of agents"""
                #  Builds msg to br
                ca_msg_br_body = opf.req_active_users_loc_times(ca_data_df)  # returns a json with request info to browser
                ca_msg_br = opf.msg_to_br(ca_msg_br_body, my_dir)  # returns a msg object with request info to browser and message setup
                await self.send(ca_msg_br)
                br_msg = await self.receive(timeout=wait_msg_time)
                if br_msg:
                    br_data_df = pd.read_json(br_msg.body)
                    br_jid = opf.br_jid(my_dir)
                    msg_sender_jid = str(br_msg.sender)
                    msg_sender_jid = msg_sender_jid[:-9]
                    if msg_sender_jid == br_jid:  # ca may receive many msgs from different agents, at this point only msg´s from br enables to continue.
                        auction_df.at[0, 'brAVG(tr_op_time)'] = br_data_df.loc[0, 'AVG(tr_op_time)']  # save to auction df
                        auction_df.at[0, 'brAVG(ca_op_time)'] = br_data_df.loc[0, 'AVG(ca_op_time)']  # save to auction df
                        """Estimation of when CA needs TR"""
                        ca_to_tr_df = opf.estimate_tr_slot(br_data_df, fab_started_at, leeway, ca_data_df)
                        """Get a df with the closest active TR to iterate to request prebook for slot 1"""
                        slot = 1
                        ca_to_tr_df.at[0, 'slot'] = slot
                        ca_to_tr_json = ca_to_tr_df.to_json()  # json to send to tr with slots to prebook
                        #print(f'br_data_df: {br_data_df}')
                        closest_tr_df = opf.get_tr_list(slot, br_data_df, my_full_name, my_dir)
                        # Create a loop to ask for availability. First loop: message to closest tr, receive answer and if available break and pre-book done. If not available, send message to next available tr.
                        jid_list = closest_tr_df['User name'].tolist()
                        auction_df.at[0, 'active_tr_slot_1'] = [closest_tr_df.to_dict()]  # save to auction df
                        ca_msg_to_tr = opf.ca_msg_to(ca_to_tr_json)
                        tr_occupied = []
                        tr_assigned = []
                        # var1, var2 = tr_assigned
                        for i in jid_list:
                            ca_msg_to_tr.to = i
                            await self.send(ca_msg_to_tr)
                            tr_msg = await self.receive(timeout=wait_msg_time)
                            if tr_msg:
                                msg_sender_jid = str(tr_msg.sender)
                                msg_sender_jid = msg_sender_jid[:-9]
                                if msg_sender_jid == i:
                                    if format(tr_msg.body) == "negative":
                                        tr_occupied.append(i)
                                        continue
                                    elif format(tr_msg.body) == "positive":
                                        tr_assigned.append(i)
                                        print("slot_1 assigned")
                                        name_tr_slot_1 = opf.get_agent_name(msg_sender_jid, my_dir)
                                        auction_df.at[0, 'name_tr_slot_1'] = name_tr_slot_1  # save to auction df
                                        auction_df.at[0, 'handling_cost_slot_1'] = opf.handling_cost(ca_to_tr_df, slot)  # save to auction df
                                        auction_df.at[0, 'slot_1_start'] = ca_to_tr_df.loc[0, 'slot_1_start']
                                        auction_df.at[0, 'slot_1_end'] = ca_to_tr_df.loc[0, 'slot_1_end']
                                        """Get a df with the closest active TR to iterate to request prebook for slot 2"""
                                        slot = 2
                                        ca_to_tr_df.at[0, 'slot'] = slot
                                        ca_to_tr_json = ca_to_tr_df.to_json()  # json to send to tr with slots to prebook
                                        closest_tr_df = opf.get_tr_list(slot, br_data_df, my_full_name, my_dir)
                                        auction_df.at[0, 'active_tr_slot_2'] = [closest_tr_df.to_dict()]  # save to auction dfdelivered_to_wh
                                        # Create a loop to ask for availability. First loop: message to closest tr, receive answer and if available break and pre-book done. If not available, send message to next available tr.
                                        jid_list = closest_tr_df['User name'].tolist()
                                        ca_msg_to_tr = opf.ca_msg_to(ca_to_tr_json)
                                        for z in jid_list:
                                            ca_msg_to_tr.to = z
                                            await self.send(ca_msg_to_tr)
                                            tr_msg = await self.receive(timeout=wait_msg_time)
                                            if tr_msg:
                                                msg_sender_jid = str(tr_msg.sender)
                                                msg_sender_jid = msg_sender_jid[:-9]
                                                if msg_sender_jid == i:
                                                    if format(tr_msg.body) == "negative":
                                                        tr_occupied.append(z)
                                                        continue
                                                    elif format(tr_msg.body) == "positive":
                                                        tr_assigned.append(z)
                                                        print("slot_2 assigned")
                                                        name_tr_slot_2 = opf.get_agent_name(msg_sender_jid, my_dir)
                                                        auction_df.at[0, 'name_tr_slot_2'] = name_tr_slot_2  # save to auction df
                                                        auction_df.at[0, 'handling_cost_slot_2'] = opf.handling_cost(ca_to_tr_df, slot)  # save to auction df
                                                        auction_df.at[0, 'slot_2_start'] = ca_to_tr_df.loc[0, 'slot_2_start']
                                                        auction_df.at[0, 'slot_2_end'] = ca_to_tr_df.loc[0, 'slot_2_end']
                                                        """Confirm bookings to slot_1 and slot_2 transports"""
                                                        l = 0
                                                        ca_to_tr_df.at[0, 'action'] = "booked"
                                                        for w in tr_assigned:
                                                            l = l + 1
                                                            if l == 1:
                                                                ca_to_tr_df.at[0, 'slot'] = 1
                                                                ca_to_tr_json = ca_to_tr_df.to_json()
                                                                ca_msg_tr_conf = opf.ca_msg_to(ca_to_tr_json)
                                                                ca_msg_tr_conf.to = w
                                                                await self.send(ca_msg_tr_conf)
                                                            elif l == 2:
                                                                ca_to_tr_df.at[0, 'slot'] = 2
                                                                ca_to_tr_json = ca_to_tr_df.to_json()
                                                                ca_msg_tr_conf = opf.ca_msg_to(ca_to_tr_json)
                                                                ca_msg_tr_conf.to = w
                                                                await self.send(ca_msg_tr_conf)
                                                            else:
                                                                """inform log"""
                                                                ca_msg_log_body = f'Error at {my_full_name}: did not confirm booking to tr agents'
                                                                ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                                                await self.send(ca_msg_log)
                                                        """inform log of tr bookings"""
                                                        ca_msg_log_json = opf.confirm_tr_bookings_to_log(ca_to_tr_df, my_full_name, closest_tr_df, tr_assigned)
                                                        ca_msg_log = opf.msg_to_log(ca_msg_log_json, my_dir)
                                                        await self.send(ca_msg_log)
                                                        auction_df.at[0, 'tr_booking_confirmation_at'] = datetime.datetime.now()  # save to auction df
                                                        """ask wh for space to store coil after fab"""
                                                        closest_wh_df = opf.get_wh_list(br_data_df, my_full_name, my_dir)
                                                        jid_list = closest_wh_df['User name'].tolist()
                                                        auction_df.at[0, 'active_wh'] = [closest_wh_df.to_dict()]  # save to auction df
                                                        ca_to_wh_df = opf.estimate_tr_slot(br_data_df, fab_started_at, leeway, ca_data_df)  # Recalculates as it has been modified
                                                        slot = 2
                                                        ca_to_wh_df.at[0, 'slot'] = slot
                                                        ca_to_wh_df.at[0, 'action'] = "book"
                                                        ca_to_wh_json = ca_to_wh_df.to_json()
                                                        ca_msg_to_wh = opf.ca_msg_to(ca_to_wh_json)
                                                        wh_occupied = []
                                                        wh_assigned = []
                                                        # var1, var2 = wh_assigned
                                                        for i in jid_list:
                                                            ca_msg_to_wh.to = i
                                                            await self.send(ca_msg_to_wh)
                                                            wh_msg = await self.receive(timeout=wait_msg_time)
                                                            if wh_msg:
                                                                msg_sender_jid = str(wh_msg.sender)
                                                                msg_sender_jid = msg_sender_jid[:-9]
                                                                if msg_sender_jid == i:
                                                                    if format(wh_msg.body) == "negative":
                                                                        wh_occupied.append(i)
                                                                        continue
                                                                    elif format(wh_msg.body) == "positive":
                                                                        wh_assigned.append(i)
                                                                        print("wh assigned")
                                                                        delivered_to_wh = opf.get_agent_name(msg_sender_jid, my_dir)
                                                                        auction_df.at[0, 'delivered_to_wh'] = delivered_to_wh  # save to auction df
                                                                        auction_df.at[0, 'wh_location'] = opf.get_agent_location(delivered_to_wh)
                                                                        """inform log of wh booking"""
                                                                        ca_msg_log_json = opf.confirm_wh_booking_to_log(ca_to_wh_df, wh_assigned, my_dir, closest_wh_df)
                                                                        ca_msg_log = opf.msg_to_log(ca_msg_log_json, my_dir)
                                                                        await self.send(ca_msg_log)
                                                                        auction_df.at[0, 'wh_booking_confirmation_at'] = datetime.datetime.now()  # Save information to auction df
                                                                        """change of status"""
                                                                        ca_status_var = "auction"  # Status change to auction in which negotiation with coil takes place
                                                                        """inform log of change of status"""
                                                                        ca_inform_json = opf.inform_log_df(my_full_name, ca_status_started_at, ca_status_var).to_json()
                                                                        ca_msg_log = opf.msg_to_log(ca_inform_json, my_dir)
                                                                        await self.send(ca_msg_log)
                                                                        auction_start = datetime.datetime.now()
                                                                        break
                                                                    else:
                                                                        print(f'{wh_msg.sender} did not set a correct msg.body: {wh_msg.body} in communication with {my_full_name}')
                                                                        """inform log of issue"""
                                                                        ca_msg_log_body = f'{wh_msg.sender} did not set a correct msg.body: {wh_msg.body} in communication with {my_full_name}'
                                                                        ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                                                        await self.send(ca_msg_log)
                                                                        print(ca_msg_log_body)
                                                                else:
                                                                    """inform log of issue"""
                                                                    ca_msg_log_body = f'{my_full_name} received a msg from {msg_sender_jid} rather than {br_jid}'
                                                                    ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                                                    await self.send(ca_msg_log)
                                                                    print(ca_msg_log_body)
                                                            else:
                                                                """inform log of issue"""
                                                                ca_msg_log_body = f'{my_full_name} did not receive answer from {i} to book wh'
                                                                ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                                                await self.send(ca_msg_log)
                                                                print(ca_msg_log_body)
                                                        else:
                                                            continue
                                                        break
                                                    else:
                                                        """inform log of issue"""
                                                        ca_msg_log_body = f'{tr_msg.sender} did not set a correct msg.body: {tr_msg.body} in communication with {my_full_name} for slot_2'
                                                        ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                                        await self.send(ca_msg_log)
                                                        print(ca_msg_log_body)
                                                else:
                                                    """inform log of issue"""
                                                    ca_msg_log_body = f'{my_full_name} received a msg from {msg_sender_jid} rather than {br_jid}'
                                                    ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                                    await self.send(ca_msg_log)
                                                    print(ca_msg_log_body)
                                            else:
                                                """inform log of issue"""
                                                ca_msg_log_body = f'{my_full_name} didn´t receive reply from {z} for slot_2'
                                                ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                                await self.send(ca_msg_log)
                                                print(ca_msg_log_body)
                                        else:
                                            continue
                                        break
                                    else:
                                        """inform log of issue"""
                                        ca_msg_log_body = f'{tr_msg.sender} did not set a correct msg.body: {tr_msg.body} in communication with {my_full_name} for slot_1'
                                        ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                        await self.send(ca_msg_log)
                                        print(ca_msg_log_body)
                                else:
                                    """inform log of issue"""
                                    ca_msg_log_body = f'{my_full_name} received a msg from {msg_sender_jid} rather than {br_jid}'
                                    ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                    await self.send(ca_msg_log)
                                    print(ca_msg_log_body)
                            else:
                                ca_msg_log_body = f'{my_full_name} did not receive answer from {i} for slot_1'
                                ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                await self.send(ca_msg_log)
                                print(ca_msg_log_body)
                    else:
                        """inform log of issue"""
                        ca_msg_log_body = f'{my_full_name} received a msg from {msg_sender_jid} rather than {br_jid}'
                        ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                        await self.send(ca_msg_log)
                        print(ca_msg_log_body)
                else:
                    """inform log"""
                    coil_msg_log_body = f'{my_full_name} did not receive any msg in the last {wait_msg_time}s at {ca_status_var}'
                    coil_msg_log = opf.msg_to_log(coil_msg_log_body, my_dir)
                    await self.send(coil_msg_log)
            elif ca_status_var == "auction":

                auction_df.at[0, 'auction_start'] = auction_start # Save information to auction df
                print(f'auction_start: {auction_start}')
                # NOW THAT WH AND TR ARE BOOKED. ENTERS AUCTION FOR COILS.
                """Asks browser for active coils and locations"""
                #  Builds msg to br
                ca_request_type = "coils"
                ca_msg_br_body = opf.req_active_users_loc_times(ca_data_df, ca_request_type)  # returns a json with request info to browser
                ca_msg_br = opf.msg_to_br(ca_msg_br_body, my_dir)
                # returns a msg object with request info to browser and message setup
                await self.send(ca_msg_br)
                br_msg = await self.receive(timeout=wait_msg_time)
                if br_msg:
                    if str(br_msg.body) == "positive" or str(br_msg.body) == "negative":
                        """Inform log """
                        ca_msg_log_body = f'{my_full_name} received answer from tc rather than browser with msg: {br_msg.body}'
                        ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                        await self.send(ca_msg_log)
                        print(ca_msg_log_body)
                    else:
                        print(f'br_msg_body: {br_msg.body}')
                        br_data_df = pd.read_json(br_msg.body)
                        br_jid = opf.br_jid(my_dir)
                        msg_sender_jid = str(br_msg.sender)
                        msg_sender_jid = msg_sender_jid[:-9]
                        """Send a message to all active coils presenting auction and ideal conditions"""
                        if msg_sender_jid == br_jid:
                            closest_coils_df = opf.get_coil_list(br_data_df, my_full_name, my_dir)
                            auction_df.at[0, 'active_coils'] = [closest_coils_df['Name'].to_list()]  # Save information to auction df
                            ca_data_df.at[0, 'auction_level'] = 1  # initial auction level
                            ca_data_df.at[0, 'bid_status'] = 'bid'
                            ca_to_coils_df = opf.ca_to_coils_initial_df(ca_data_df, plc_temp_df)
                            ca_to_coils_json = ca_to_coils_df.to_json()  # json to send to coils with auction info including last temperatures
                            # Create a loop to inform of auctionable resource to willing to be fab coils.
                            jid_list = closest_coils_df['User name'].tolist()
                            ca_msg_to_coils = opf.ca_msg_to(ca_to_coils_json)
                            for z in jid_list:
                                ca_msg_to_coils.to = z
                                await self.send(ca_msg_to_coils)
                            """Create a loop to receive all* the messages"""
                            coil_msgs_df = pd.DataFrame()
                            print(range(len(jid_list)))
                            for i in range(len(jid_list)):  # number of messages that enter auction
                                coil_msg = await self.receive(timeout=wait_msg_time / len(jid_list))
                                if coil_msg:
                                    msg_sender_jid0 = str(coil_msg.sender)
                                    print(f'msg_sender_jid0: {msg_sender_jid0}')
                                    msg_sender_jid = msg_sender_jid0[:-40]
                                    print(f'msg_sender_jid-40: {msg_sender_jid}')
                                    msg_sender_jid = msg_sender_jid0[:-33]
                                    print(f'msg_sender_jid-33: {msg_sender_jid}')
                                    if msg_sender_jid == "c0":
                                        coil_msg_df = pd.read_json(coil_msg.body)
                                        coil_jid = str(coil_msg.sender)
                                        coil_jid = coil_jid[:-9]
                                        coil_msg_df.at[0, 'coil_jid'] = coil_jid
                                        coil_msgs_df = coil_msgs_df.append(coil_msg_df)  # received msgs
                                        print('received msgs from coils')
                                    else:
                                        """inform log of issue"""
                                        ca_msg_log_body = f'{my_full_name} received a msg from {msg_sender_jid} rather than coil'
                                        ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                        await self.send(ca_msg_log)
                                        print(ca_msg_log_body)
                                else:
                                    """Inform log """
                                    ca_msg_log_body = f'{my_full_name} did not receive answer from coil'
                                    ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                    await self.send(ca_msg_log)
                                    print(ca_msg_log_body)
                            if not coil_msgs_df.empty:
                                """Evaluate proposal and give a rating"""
                                ca_data_df.at[0, 'auction_level'] = 2  # Second level
                                auction_df.at[0, 'auction_coils'] = [coil_msgs_df['id'].to_list()]  # Send info to log
                                bids_ev_df = opf.auction_bid_evaluation(coil_msgs_df, ca_data_df)  # Returns a df with an extra column with rating to coils proposal.
                                auction_df.at[0, 'coil_ratings_1'] = [bids_ev_df.to_dict()]
                                print('rating given to received msgs from coils')
                                print(f'bids_ev_df: {bids_ev_df.to_string()}')
                                print(f'coil_msgs_df: {coil_msgs_df.to_string()}')
                                """Evaluate if negotiation is needed"""
                                ca_counter_bid_df = opf.ca_negotiate(bids_ev_df, coil_msgs_df)  # Returns a df with coils to send message asking to counterbid
                                print(ca_counter_bid_df.to_string())
                                if len(ca_counter_bid_df['coil_jid']) >= 2:  # at least 2 coils have similar rating.
                                    for i in ca_counter_bid_df['coil_jid'].tolist():
                                        """Ask for extra bid"""
                                        ca_data_df.at[0, 'bid_status'] = 'extrabid'
                                        ca_coil_extra_msg = opf.ca_msg_to(ca_data_df.to_json())
                                        ca_coil_extra_msg.to = i
                                        await self.send(ca_coil_extra_msg)
                                    """Create a loop to receive all the messages"""
                                    coil_msgs_df = pd.DataFrame()
                                    for i in range(len(ca_counter_bid_df['coil_jid'])):
                                        coil_msg = await self.receive(timeout=wait_msg_time / len(ca_counter_bid_df['coil_jid']))
                                        if coil_msg:
                                            msg_sender_jid0 = str(coil_msg.sender)
                                            print(f'msg_sender_jid0_: {msg_sender_jid0}')
                                            msg_sender_jid = msg_sender_jid0[:-40]
                                            print(f'msg_sender_jid-40_: {msg_sender_jid}')
                                            msg_sender_jid = msg_sender_jid0[:-33]
                                            print(f'msg_sender_jid-33_: {msg_sender_jid}')
                                            print(msg_sender_jid)
                                            if msg_sender_jid == "c0":
                                                coil_msg_df = pd.read_json(coil_msg.body)
                                                coil_jid = coil_msg.sender
                                                coil_msg_df.at[0, 'coil_jid'] = coil_jid
                                                coil_msgs_df = coil_msgs_df.append(coil_msg_df)  # received msgs
                                            else:
                                                """inform log of issue"""
                                                ca_msg_log_body = f'{my_full_name} received a msg from {msg_sender_jid} rather than coil'
                                                ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                                await self.send(ca_msg_log)
                                                print(ca_msg_log_body)
                                        else:
                                            """Inform log """
                                            ca_msg_log_body = f'{my_full_name} did not receive answer from any coil'
                                            ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                            await self.send(ca_msg_log)
                                            print(ca_msg_log_body)
                                    if not coil_msgs_df.empty:
                                        """Evaluate extra bids and give a rating"""
                                        ca_data_df.at[0, 'auction_level'] = 3  # third level
                                        bids_ev_df_2 = opf.auction_bid_evaluation(coil_msgs_df, ca_data_df)  # Returns a df with an extra column with rating to coils proposal.
                                        coil_jid_winner_df = opf.ca_negotiate(bids_ev_df, coil_msgs_df)
                                        """Inform coil of assignation and agree on assignation"""
                                        # Iterate over finalist bidders to confirm assignation
                                        for i in range(len(coil_jid_winner_df['coil_jid'])):
                                            coil_jid_winner = coil_jid_winner_df.loc[i, 'coil_jid']
                                            print(f'coil_jid_winner_2: {coil_jid_winner}')
                                            coil_jid_winner = str(coil_jid_winner)
                                            print(f'coil_jid_winner_2_: {coil_jid_winner}')
                                            ca_data_df.at[0, 'bid_status'] = 'acceptedbid'
                                            ca_coil_winner_msg = opf.ca_msg_to(ca_data_df.to_json())
                                            ca_coil_winner_msg.to = coil_jid_winner[:-9]
                                            await self.send(ca_coil_winner_msg)
                                            # Receive answer and check if confirmation of assignation is done
                                            coil_msg = await self.receive(timeout=wait_msg_time)
                                            if coil_msg:
                                                msg_sender_jid0 = str(coil_msg.sender)
                                                print(f'msg_sender_jid0_: {msg_sender_jid0}')
                                                msg_sender_jid = msg_sender_jid0[:-40]
                                                print(f'msg_sender_jid-40_: {msg_sender_jid}')
                                                msg_sender_jid = msg_sender_jid0[:-33]
                                                print(f'msg_sender_jid-33_: {msg_sender_jid}')
                                                print(msg_sender_jid)
                                                if msg_sender_jid == "c0":
                                                    coil_msg_df = pd.read_json(coil_msg.body)
                                                    coil_jid = coil_msg.sender
                                                    coil_msg_df.at[0, 'coil_jid'] = coil_jid
                                                    if coil_msg_df.loc[0, 'bid_status'] == 'acceptedbid':
                                                        """Save winner information"""
                                                        auction_df.at[0, 'coil_ratings_2'] = [bids_ev_df_2.to_dict()]  # Save information to auction df
                                                        """Calculate processing time"""
                                                        process_df = opf.process_df(process_df, coil_jid_winner_df, ca_to_tr_df)
                                                        """Inform log of assignation and auction KPIs"""
                                                        ca_msg_log_body = opf.auction_kpis(ca_data_df, bids_ev_df_2, auction_df, process_df, ca_counter_bid_df, coil_jid_winner_df)
                                                        print(f'ca_msg_log_body:{ca_msg_log_body}')
                                                        ca_msg_log_body_json = ca_msg_log_body.to_json()
                                                        print(f'ca_msg_log_body_json:{ca_msg_log_body_json}')
                                                        ca_msg_log = opf.msg_to_log(ca_msg_log_body_json, my_dir)
                                                        op_times_df.at[0, 'AVG(ca_op_time)'] = ca_msg_log_body.loc[0, 'AVG(ca_op_time)']
                                                        op_times_df.at[0, 'AVG(tr_op_time)'] = ca_msg_log_body.loc[0, 'AVG(tr_op_time)']
                                                        await self.send(ca_msg_log)
                                                        """change status to stand-by until next auction"""
                                                        ca_status_var = "stand_by"
                                                        ca_data_df = opf.modify_ca_data_df(process_df, ca_data_df)  # modify ca_data_df
                                                        plc_temp_df = ca_data_df[['T1', 'T2', 'T3', 'T4', 'T5', 'q']]
                                                        fab_started_at = process_df['fab_start'].iloc[-1]
                                                        break
                                                    else:
                                                        """Inform log """
                                                        ca_msg_log_body = f'{my_full_name} did not receive answer from finalist coil'
                                                        ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                                        await self.send(ca_msg_log)
                                                        print(ca_msg_log_body)
                                                else:
                                                    """inform log of issue"""
                                                    ca_msg_log_body = f'{my_full_name} received a msg from {msg_sender_jid} rather than coil'
                                                    ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                                    await self.send(ca_msg_log)
                                                    print(ca_msg_log_body)
                                            else:
                                                """Inform log """
                                                ca_msg_log_body = f'{my_full_name} did not receive answer from coil'
                                                ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                                await self.send(ca_msg_log)
                                                print(ca_msg_log_body)
                                    else:
                                        """Inform log """
                                        ca_msg_log_body = f'{my_full_name} did not receive answer from any coil'
                                        ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                        await self.send(ca_msg_log)
                                        print(ca_msg_log_body)
                                elif len(ca_counter_bid_df['coil_jid']) == 1:  # if there is a strong winner
                                    """Inform coil of assignation"""
                                    ca_data_df.at[0, 'bid_status'] = 'acceptedbid'
                                    coil_jid_winner = ca_counter_bid_df.loc[0, 'coil_jid']
                                    ca_coil_winner_msg = opf.ca_msg_to(ca_data_df.to_json())
                                    print(ca_coil_winner_msg)
                                    print(f'coil_jid_winner_1: {coil_jid_winner}')
                                    coil_jid_winner = str(coil_jid_winner)
                                    print(f'coil_jid_winner_1_: {coil_jid_winner}')
                                    ca_coil_winner_msg.to = coil_jid_winner[:-9]
                                    await self.send(ca_coil_winner_msg)
                                    for i in ca_counter_bid_df['coil_jid'].tolist():
                                        """Ask for confirmation"""
                                        ca_coil_extra_msg = opf.ca_msg_to(ca_data_df.to_json())
                                        ca_coil_extra_msg.to = i
                                        await self.send(ca_coil_extra_msg)
                                    """Create a loop to receive all the messages"""
                                    coil_msgs_df = pd.DataFrame()
                                    coil_msg = await self.receive(timeout=wait_msg_time / len(ca_counter_bid_df['coil_jid']))
                                    if coil_msg:
                                        msg_sender_jid0 = str(coil_msg.sender)
                                        print(f'msg_sender_jid0_: {msg_sender_jid0}')
                                        msg_sender_jid = msg_sender_jid0[:-40]
                                        print(f'msg_sender_jid-40_: {msg_sender_jid}')
                                        msg_sender_jid = msg_sender_jid0[:-9]
                                        print(f'msg_sender_jid-9_: {msg_sender_jid}')
                                        print(msg_sender_jid)
                                        if msg_sender_jid == coil_jid_winner:
                                            coil_msg_df = pd.read_json(coil_msg.body)
                                            if coil_msg_df.loc[0, 'bid_status'] == "acceptedbid":
                                                """Calculate processing time"""
                                                print(bids_ev_df.to_string())
                                                process_df = opf.process_df(process_df, ca_counter_bid_df, ca_to_tr_df)
                                                """Inform log of assignation and auction KPIs"""
                                                ca_msg_log_body = opf.auction_kpis(ca_data_df, bids_ev_df, auction_df, process_df, ca_counter_bid_df)  # in this case, counterbid_df only contains 1 row with winner info
                                                ca_msg_log = opf.msg_to_log(ca_msg_log_body.to_json(), my_dir)
                                                op_times_df.at[0, 'AVG(ca_op_time)'] = ca_msg_log_body.loc[0, 'AVG(ca_op_time)']
                                                op_times_df.at[0, 'AVG(tr_op_time)'] = ca_msg_log_body.loc[0, 'AVG(tr_op_time)']
                                                await self.send(ca_msg_log)
                                                """change status to stand-by until next auction"""
                                                ca_status_var = "stand_by"
                                                ca_data_df = opf.modify_ca_data_df(process_df, ca_data_df)  # modify ca_data_df
                                                print(f'process_df: {process_df.to_string()}')
                                                print(f'ca_data_df: {ca_data_df.to_string()}')
                                                plc_temp_df = ca_data_df[['T1', 'T2', 'T3', 'T4', 'T5', 'q']]
                                                print(f'plc_temp_df: {plc_temp_df.to_string()}')
                                                fab_started_at = process_df['fab_start'].iloc[-1]
                                            else:
                                                """inform log of issue"""
                                                coil_id = coil_msg_df.loc[0, 'id']
                                                ca_msg_log_body = f'{coil_id} rejected final acceptance to {my_full_name}'
                                                ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                                await self.send(ca_msg_log)
                                                print(ca_msg_log_body)
                                        else:
                                            """inform log of issue"""
                                            ca_msg_log_body = f'{my_full_name} received a msg from {msg_sender_jid} rather than {coil_jid_winner}'
                                            ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                            await self.send(ca_msg_log)
                                            print(ca_msg_log_body)
                                    else:
                                        """Inform log """
                                        ca_msg_log_body = f'{my_full_name} did not receive answer from {coil_jid_winner}'
                                        ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                        await self.send(ca_msg_log)
                                        print(ca_msg_log_body)
                                else:
                                    """inform log of issue"""
                                    ca_msg_log_body = f'{my_full_name} error on evaluation if negotiation is needed'
                                    ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                    await self.send(ca_msg_log)
                                    print(ca_msg_log_body)
                            else:
                                """Inform log """
                                ca_msg_log_body = f'{my_full_name} did not receive answer from any coil. coils_msgs_df is empty'
                                ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                                await self.send(ca_msg_log)
                                print(ca_msg_log_body)
                        else:
                            """inform log of issue"""
                            ca_msg_log_body = f'{my_full_name} received a msg from {msg_sender_jid} rather than {br_jid}'
                            ca_msg_log = opf.msg_to_log(ca_msg_log_body, my_dir)
                            await self.send(ca_msg_log)
                            print(ca_msg_log_body)
                else:
                    """inform log"""
                    coil_msg_log_body = f'{my_full_name} did not receive any msg in the last {wait_msg_time}s at {ca_status_var}'
                    coil_msg_log = opf.msg_to_log(coil_msg_log_body, my_dir)
                    await self.send(coil_msg_log)
            elif ca_status_var == "stand-by": # stand-by status for CA is very useful. It changes to pre-auction, when there are 3 minutes left to the end of current processing.
                """inform log of status"""
                ca_inform_json = opf.inform_log_df(my_full_name, ca_status_started_at, ca_status_var).to_json()
                ca_msg_log = opf.msg_to_log(ca_inform_json, my_dir)
                await self.send(ca_msg_log)
                """Starts next auction when there is some time left before current fab ends"""
                if process_df['start_next_auction_at'].iloc[-1] < datetime.datetime.now():
                    ca_status_var = 'pre-auction'
            else:
                """inform log of status"""
                ca_inform_json = opf.inform_log_df(my_full_name, ca_status_started_at, ca_status_var).to_json()
                ca_msg_log = opf.msg_to_log(ca_inform_json, my_dir)
                await self.send(ca_msg_log)
                ca_status_var = "stand-by"

        async def on_end(self):
            print({self.counter})

        async def on_start(self):
            self.counter = 1

    async def setup(self):
        start_at = datetime.datetime.now() + datetime.timedelta(seconds=3)
        b = self.CABehav(period=3, start_at=start_at)  # periodic sender
        template = Template()
        template.metadata = {"performative": "inform"}
        self.add_behaviour(b)


if __name__ == "__main__":
    """Parser parameters"""
    parser = argparse.ArgumentParser(description='wh parser')
    parser.add_argument('-an', '--agent_number', type=int, metavar='', required=False, default=1, help='agent_number: 1,2,3,4..')
    parser.add_argument('-w', '--wait_msg_time', type=int, metavar='', required=False, default=20, help='wait_msg_time: time in seconds to wait for a msg')
    parser.add_argument('-st', '--stop_time', type=int, metavar='', required=False, default=84600, help='stop_time: time in seconds where agent')
    parser.add_argument('-s', '--status', type=str, metavar='', required=False, default='stand-by', help='status_var: on, stand-by, Off')
    parser.add_argument('-sab', '--start_auction_before', type=int, metavar='', required=False, default=10, help='start_auction_before: seconds to start auction prior to current fab ends')
    args = parser.parse_args()
    my_dir = os.getcwd()
    my_name = os.path.basename(__file__)[:-3]
    my_full_name = opf.my_full_name(my_name, args.agent_number)
    wait_msg_time = args.wait_msg_time
    ca_status_started_at = datetime.datetime.now().time()
    ca_status_refresh = datetime.datetime.now() + datetime.timedelta(seconds=5)
    ca_status_var = args.status
    start_auction_before = args.start_auction_before
    """Save to csv who I am"""
    opf.set_agent_parameters(my_dir, my_name, my_full_name)
    ca_data_df = pd.read_csv(f'{my_full_name}.csv', header=0, delimiter=",", engine='python')
    plc_temp_df = ca_data_df[['T1', 'T2', 'T3', 'T4', 'T5', 'q']]
    auction_df = opf.auction_blank_df()
    process_df = pd.DataFrame([], columns=['fab_start', 'processing_time', 'start_auction_before', 'start_next_auction_at', 'fab_end', 'T1', 'T2', 'T3', 'T4', 'T5', 'q'])
    process_df.at[0, 'start_next_auction_at'] = datetime.datetime.now() + datetime.timedelta(seconds=start_auction_before)
    fab_started_at = datetime.datetime.now()
    leeway = datetime.timedelta(minutes=int(2))  # with fab process time ranging between 8-10 min, and tr op time between 3-4 min. Max dif between estimation and reality is 3min.
    op_times_df = pd.DataFrame([], columns=['AVG(ca_op_time)', 'AVG(tr_op_time)'])
    auction_start = ""
    ca_to_tr_df = pd.DataFrame()
    """XMPP info"""
    ca_jid = opf.agent_jid(my_dir, my_full_name)
    ca_passwd = opf.agent_passwd(my_dir, my_full_name)
    ca_agent = ContinuousAnnealingAgent(ca_jid, ca_passwd)
    future = ca_agent.start(auto_register=True)
    future.result()
    """Counter"""
    stop_time = datetime.datetime.now() + datetime.timedelta(seconds=args.stop_time)
    while datetime.datetime.now() < stop_time:
        time.sleep(1)
    else:
        ca_status_var = "off"
        ca_agent.stop()
        quit_spade()


