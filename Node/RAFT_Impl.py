import time
import threading
import socket
import json
import traceback
from queue import Queue
import os 

common_message_store = []
incoming_heartbeat_list = []
send_this_message_list = []
vote_request_list = []
vote_recieve_list = []
node_name=os.environ['NODE_NAME']
# node_name=5555
VOTE = {"sender_name": node_name, "request":"VOTE_ACK", "key":"", "value":"", "term": 1}
vote_request={"term": 1, "sender_name": node_name, "request":"VOTE_REQUEST", "key":"", "value":"", "lastLogIndex": 0, "lastLogTerm": 0}
global_state={"currentTerm": 1, "votedFor": None, "log": [], "timeout_interval": None, "heartbeat_interval": 1000}
heartbeat_rpc={"sender_name": node_name, "request": "APPEND_RPC", "key": "", "value": "", "entries": [], "term": 1, "prevLogIndex": 0, "prevLogTerm": 0}
state='follower'
current_leader=None
timeout_req=False
current_term = global_state['currentTerm']
# other_nodes = [5556, 5557]
other_nodes = os.environ["NODES"].split(";")
stop_threads = False
custom_target = None

stop_timer=False

def timer_func(interval):
    ''' 
    '''
    #print("inside timer_func!!")
    #start_time=time.time()
    global stop_timer
    inter_sec=int(global_state[interval])
    inter=int(inter_sec/10)
    for i in range (inter):
        # print("Waiting")
        if(stop_timer):
            break
            #print("inside if!!")
        time.sleep(1/1000)
    #end_time=time.time()
    #print("Done!")
    #print(end_time-start_time)
    
def listener(skt):
    '''
    Thread 1 - Listener Thread
    When: Always
    Recevied the request and add it to the queue,
    so that the messages are not lost
    '''
    global stop_threads
    print(f"Starting Listener ")
    while True:
        try:
            msg, addr = skt.recvfrom(1024)
        except Exception as e:
            print(f"ERROR while fetching from socket : {traceback.print_exc()}", e.with_traceback())

        # Decoding the Message received from Node 1
        decoded_msg = json.loads(msg.decode('utf-8'))
        print(f"Message Received : {decoded_msg} From : {addr}")
        common_message_store.append(decoded_msg)

        if 'counter' in decoded_msg.keys():
            if decoded_msg['counter'] >= 4:
                print("Exiting Listener Function")
                global stop_threads
                stop_threads = True
                break

def rpc_sender(skt):
    '''
    Thread 2 - Sender Thread
    '''
    print("Starting Sender")
    global stop_threads
    global custom_target
    while True:
        if send_this_message_list:            
            msg_bytes = json.dumps(send_this_message_list.pop(0)).encode('utf-8')
            # Currently hardcoded
            if custom_target is None:
                for target in other_nodes:
                    try:
                        skt.sendto(msg_bytes, (target, 5555))
                        # skt.sendto(msg_bytes, ('localhost', target))                        
                    except Exception as e:
                        continue
            else:
                #print("Send msg", msg_bytes)                
                # skt.sendto(msg_bytes, ('localhost', custom_target))
                skt.sendto(msg_bytes, (custom_target, 5555))
                custom_target = None

        if stop_threads:
            print("Stopping Sender")
            break

def leader_state():
    '''
    '''
    global stop_timer
    global state
    stop_timer = False
    timer = threading.Thread(target=timer_func, args=['heartbeat_interval'])
    timer.start()
    while state == 'leader':
        if not timer.is_alive():
            send_this_message_list.append(heartbeat_rpc)

def main_process():
    '''
    Thread 3 - Processor Thread
    '''
    print("Starting processor")
    global stop_threads
    global state
    while True:
        if state=='follower':
            follower_state()
        elif state=='candidate':
            modify_term()
            candidate_state()
        elif state=='leader':
            leader_state()
        if stop_threads:
            print("Stopping processor")
            break

def candidate_state():
    '''
    '''    
    global stop_timer
    global current_term
    global state            
    vote_recieve_list.clear()
    vote_recieve_list.append(VOTE)
    if global_state['votedFor'] is None:
        global_state['votedFor'] = "Self"
    send_this_message_list.append(vote_request)
   
    stop_timer = False
    timer = threading.Thread(target=timer_func, args=['timeout_interval'])
    timer.start()
    while timer.is_alive():
        if (len(vote_recieve_list) > (len(other_nodes) + 1 ) / 2) and state=='candidate':
            state = 'leader'
            send_this_message_list.append(heartbeat_rpc)
            stop_timer = True
            break
            #return True
        elif incoming_heartbeat_list:
            leader_term=incoming_heartbeat_list.pop(0)
            if (leader_term['term']>=current_term):
                stop_timer = True
                state = 'follower'
                break
            #else:
                #hold_election()
            #return True
    #return False
   
def follower_state():
    '''
    '''
    global stop_timer
    global state
    global current_leader
    global current_term
    global timeout_req
    stop_timer = False
    timer = threading.Thread(target=timer_func, args=['timeout_interval'])
    timer.start()
    while timer.is_alive():
        if incoming_heartbeat_list:
            heartbeat=incoming_heartbeat_list.pop(0)
            current_leader=heartbeat['sender_name']
            if(heartbeat['term']>=current_term):
                stop_timer = True
                break
    if not incoming_heartbeat_list and not stop_timer:
        state='candidate'
            #return True
    #return False

def state_monitor():
    '''
    '''
    global stop_threads
    global state
    while True:
        print(state)
        time.sleep(0.001)
        if stop_threads:
            print("Stopping Sender")
            break        

def message_processor():
    '''
    Thread 4 - Messaging
    TODO - Only vote if follower/candidate
    '''
    global state
    global stop_threads    
    while True:
        if common_message_store:
            popped_message = common_message_store.pop(0)
            #print(popped_message)
            if popped_message['request'] == 'APPEND_RPC':
                incoming_heartbeat_list.append(popped_message)
                #current_state = 'follower'
            elif popped_message['request'] == 'VOTE_REQUEST':
                vote_request_list.append(popped_message)
                hold_election()
            else:
                vote_recieve_list.append(popped_message)

        if stop_threads:
            print("Stopping Sender")
            break

def processor_thread():
    '''
    Thread 3 - Processor Thread
    '''
    print("Starting processor")
    global stop_threads
    while True:
        main_process()
        if stop_threads:
            print("Stopping processor")
            break

def modify_name():
    '''
    '''
    global node_name
    heartbeat_rpc['sender_name'] = node_name
    vote_request['sender_name'] = node_name
    VOTE['sender_name'] = node_name

    global_state['timeout_interval'] = int(os.environ['TIMEOUT_VALUE'])
    # global_state['timeout_interval'] = 2400

    # with open('message.json', 'w') as outfile:
    #     json.dump(message_template, outfile)
    # outfile = None

    # with open('voting_template.json', 'w') as outfile:
    #     json.dump(voting_template, outfile)
    # outfile = None

    # with open('state.json', 'w') as outfile:
    #     json.dump(current_context, outfile)
    # outfile = None

    # with open('voting_ack.json', 'w') as outfile:
    #     json.dump(ACK_DICT, outfile)

def hold_election():
    '''
    '''
    global custom_target
    global current_term
    while vote_request_list:

        popped_message = vote_request_list.pop(0)
        print("Voted for", global_state['votedFor'])
        if ((popped_message['term'] > current_term) and global_state['votedFor'] is None):
            global_state['votedFor'] = popped_message['sender_name']            
            custom_target = popped_message['sender_name']
            send_this_message_list.append(VOTE)
            modify_term()

def modify_term():
    '''
    '''
    print("Modifing term")
    global current_term
    current_term = current_term + 1
    global_state['currentTerm'] = current_term
    #current_context['term'] = current_term
    vote_request['term'] = current_term
    heartbeat_rpc['term'] = current_term
    global_state['votedFor'] = None

    # with open('Node/message.json', 'w') as outfile:
    #     json.dump(heartbeat_rpc, outfile)
    # outfile = None


    # with open('Node/voting_template.json', 'w') as outfile:
    #     json.dump(vote_request, outfile)
    # outfile = None

    # with open('Node/state.json', 'w') as outfile:
    #     json.dump(global_state, outfile)

if __name__=="__main__":
    '''
    '''
    source = node_name
    modify_name()
    # source = 'localhost'
    UDP_Skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDP_Skt.bind((source, 5555))

    managing_thread = threading.Thread(target=main_process) 
    listen_thread = threading.Thread(target=listener, args=[UDP_Skt])
    send_thread = threading.Thread(target=rpc_sender, args=[UDP_Skt])
    message_thread = threading.Thread(target=message_processor)
    state_thread = threading.Thread(target=state_monitor)

    message_thread.start()
    listen_thread.start()
    send_thread.start()
    managing_thread.start()
    state_thread.start()
