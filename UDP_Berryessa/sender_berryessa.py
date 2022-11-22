#! python3
# sender_berryessa.py
# usage: python3 sender_berryessa.py --dest_ip XXXX.XXXX.XXXX.XXXX
# --dest_port YYYY --tcp_version tahoe/reno --input input.txt

import socket
import argparse
import zlib
import struct
import random
import time
import os
import threading
from queue import Queue
from datetime import datetime


# header size
HEADER_SIZE = 31
# signal to kill threads, or not, depending on their behavior
kill_threads = threading.Event()
# port the client will use
client_port = 0
# create argument parser
parser = argparse.ArgumentParser()
# add required dest_ip argument
parser.add_argument('-s', '--dest_ip', type=str, required=True)
# add the required dest_port argument
parser.add_argument('-p', '--dest_port', type=int, required=True)
# add the required input argument
parser.add_argument('-i', '--input', type=str, required=True)
# add the required tcp_version argument
parser.add_argument('-t', '--tcp_version', type=str)
# parse the arguments
args = parser.parse_args()
# ip address or hostname of the hose
host = args.dest_ip
# port the host is listening for connections on
port = args.dest_port
# BUFFER_SIZE
BUFFER_SIZE = 1000
# message to be sent to server
message = "Ping"
# Sequence to use for the file transfer
SEQ = random.getrandbits(32)
# set the initial TIMEOUT_INTERVAL
TIMEOUT_INTERVAL = .002
# the total number of timeouts/packet loss for this session
TIMEOUT_COUNT = 0
# used to compare packets lost between CWND transmits
TIMEOUT_COMPARE = 0
# the total number of packets sent for this session
PACKET_COUNT = 0
# CWND
CWND = 1
# Initial CWND
INIT_CWND = CWND
# ssthresh
ssthresh = 16
# transmission round
TRANSMISSION_ROUND = 0
# congestion control state
CONGESTION_STATE = "SLOW START"


class ServerThread(threading.Thread):
    def __init__(self, send_queue, resend_queue):
        threading.Thread.__init__(self)
        self.send_queue = send_queue
        self.resend_queue = resend_queue

    def run(self):
        packet, saddress, r_sock = self.send_queue.get()
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                send_and_wait_respond(s, packet, saddress, r_sock)
        except KeyboardInterrupt:
            close(s, saddress)
            close(r_sock, saddress)
            kill_threads.set()
        except TimeoutError:
            self.resend_queue.put((packet, saddress, r_sock))
        finally:
            self.send_queue.task_done()

    def join(self, timeout=None):
        threading.Thread.join(self, timeout)


def timestamp():
    timestamp = time.time()
    return datetime.utcfromtimestamp(timestamp).strftime('%Y%m%d%H%M%S%f')


def file_logging(msg, file):
    with open(file, 'a') as write_file:
        write_file.write(msg + '\n')
    #print(msg)


log_file = f"sender_berryessa_{timestamp()}.log"
log_file_path = os.path.abspath(os.path.join('.', log_file))
stats_file = f"sender_berryessa_{timestamp()}_TRAN_CWND.log"
stats_file_path = os.path.abspath(os.path.join('.', stats_file))
final_stats_file = f"sender_berryessa_{timestamp()}_final_stats.log"
final_stats_file_path = os.path.abspath(os.path.join('.', final_stats_file))


# calculate a new timeout interval
def timeout_calc(sec=0.0, pack_l=False, calc=False, triple_ack=False):
    global TIMEOUT_INTERVAL
    global TIMEOUT_COUNT
    global TIMEOUT_COMPARE
    global CWND
    global ssthresh
    global CONGESTION_STATE
    if pack_l:
        TIMEOUT_COUNT += 1
    if calc:
        print("WINDOW Processed")
        if TIMEOUT_COUNT > TIMEOUT_COMPARE:  # packet loss in the most recent window
            pack_l = True
            TIMEOUT_COMPARE = TIMEOUT_COUNT
        if args.tcp_version == 'tahoe':
            if pack_l:
                ssthresh = int(CWND/2)
                CWND = 1
                CONGESTION_STATE = "SLOW START"
            elif CWND < ssthresh:
                CWND = CWND * 2
                CONGESTION_STATE = "FAST RETRANSMIT"
            elif CWND >= ssthresh:
                CWND += 1
                CONGESTION_STATE = "AIMD"
        elif args.tcp_version == 'reno':
            if triple_ack or pack_l:
                CWND = int(CWND / 2)
                ssthresh = int(CWND / 2)
                CONGESTION_STATE = "FAST RECOVERY"
            elif CWND < ssthresh:
                CWND = CWND * 2
                CONGESTION_STATE = "FAST RETRANSMIT"
            elif CWND >= ssthresh:
                CWND += 1
                CONGESTION_STATE = "AIMD"
        else:
            t = .5
            print(f"TIMEOUT INTERVAL = {TIMEOUT_INTERVAL}\n Packet Loss = {TIMEOUT_COUNT}\nPackets Sent = {PACKET_COUNT}")
            if TIMEOUT_COUNT == 1:
                TIMEOUT_INTERVAL = sec
            else:
                TIMEOUT_INTERVAL = (1.0-t) * TIMEOUT_INTERVAL + (t*sec)


# checksum calculator for ensuring our data is being transferred
def checksum_calc(data):
    checksum = zlib.crc32(data)
    return checksum


def connect(conn_host, conn_port):
    server = (conn_host, conn_port)
    # initialize the socket to use for incoming
    in_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # initialize the socket to use for outgoing
    out_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # bind the incoming socket to listen for connections from the server
    in_sock.bind(('localhost', client_port))
    print(f"Establishing connection to {server}")
    # create the header
    first_handshake_header_p, first_handshake_header = header(in_sock.getsockname()[1], conn_port,
                                                              ''.encode(), SEQ, syn=True)
    # send the header to the server
    in_header, in_data, in_address = send_and_wait_respond(out_sock, first_handshake_header_p,
                                                           server, in_sock)
    if in_header[6] and in_header[7] and (in_header[5] == SEQ + 1):
        tcp_port = client_port
        # initialize the socket to use for the connection
        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # bind the incoming socket to listen for connections from the server
        tcp_sock.bind(('localhost', tcp_port))
        second_handshake_header_p, second_handshake_header = header(tcp_sock.getsockname()[1], conn_port, ''.encode(), SEQ,
                                                                    ack_seq=in_header[5] + 1,
                                                                    ack=True)
        send(out_sock, second_handshake_header_p, server)
        print(f"binding listener socket to localhost:{tcp_port}")

        in_sock.close()
        out_sock.close()
        return tcp_sock, (server[0], in_header[0])
    else:
        in_sock.close()
        out_sock.close()
        return None, None


def close(sock, address):
    print(f"Disconnecting from: {address}")
    try:
        data = ''.encode()
        close_header_p, close_header = header(sock.getsockname()[1], address[1], data,
                                              0, fin=True)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                r_header, r_data, r_address = send_and_wait_respond(s, close_header_p + data,
                                                                    address, sock)
            if r_header[6] and r_header[8]:
                print("server responded to disconnect.")
                sock.close()
            else:
                print("else")
                close(sock, address)
        except:
            sock.close()
        print("Finished closing connection.")
    except OSError:
        pass


def send(s, packet, address):
    global PACKET_COUNT
    PACKET_COUNT += 1
    s.sendto(packet, address)
    header = packet[:HEADER_SIZE]
    header = unpack_header(header)
    print_header(header)
    file_logging(f"{header[0]} | {header[1]} | {msg_type(header)} | {header[2]} | {header[9]} | {timestamp()}", log_file_path)


def send_and_wait_respond(s, packet, address, re_sock):
    global TIMEOUT_INTERVAL
    # for i in range(0, 3):
    while True:
        re_sock.settimeout(TIMEOUT_INTERVAL)
        t1 = time.time()
        try:
            send(s, packet, address)
            s_header = packet[:HEADER_SIZE]
            s_header = unpack_header(s_header)
            r_header, r_data, r_address = receive(re_sock)
            t2 = time.time()
            timeout_calc(t2-t1)
            if r_header[5] == s_header[5] and not r_header[8]:  # data not received by receiver, it requested the same packet
                timeout_calc(calc=True, triple_ack=True)
                return send_and_wait_respond(s, packet, address, re_sock)
            else:
                return r_header, r_data, r_address
        except KeyboardInterrupt:
            close(re_sock, address)
            break
        except:  # timed out try sending the same packet again
            t2 = time.time()
            timeout_calc(t2-t1, pack_l=True)
    raise TimeoutError


def receive(rec_sock, buffer=BUFFER_SIZE):
    r, r_address = rec_sock.recvfrom(buffer)
    r_header = r[:HEADER_SIZE]
    r_header = unpack_header(r_header)
    data = r[HEADER_SIZE:]
    file_logging(f"{r_header[0]} | {r_header[1]} | {msg_type(r_header)} | {r_header[2]} | {r_header[9]} | {timestamp()}", log_file_path)
    print_header(r_header)
    return r_header, data, r_address


def header(sender_port, receiver_port, data, seq, ack_seq=0, ack=False, syn=False, fin=False):
    # create the header ('struct format', 'sender port', 'receiver port', 'win',
    # 'checksum', 'seq', 'ack seq', 'ack', 'syn', 'fin', 'bdp')
    win = len(data)+HEADER_SIZE
    checksum = checksum_calc(data)
    if ack_seq == 0:
        ack_seq = seq
    header_packed = struct.pack("!IIIIII???I", sender_port, receiver_port, win, checksum, seq,
                                ack_seq, ack, syn, fin, CWND * BUFFER_SIZE)
    return header_packed, unpack_header(header_packed)


def unpack_header(header):
    return struct.unpack("!IIIIII???I", header)


def msg_type(header):
    if header[2] > HEADER_SIZE:
        return "DATA"
    elif header[6] and header[7]:
        return "ACK/SYN"
    elif header[6]:
        return "ACK"
    elif header[7]:
        return "SYN"
    elif header[8]:
        return "FIN"


def print_header(header):
    print(f"{header[0]} | {header[1]} | {header[2]} | {header[3]} | {header[4]} | {header[5]} | {header[6]} | {header[7]} | {header[8]} | {header[9]}")


send_window = Queue()
resend_window = Queue()

tcp_sock, address = connect(host, port)
print(f"handhsake complete, server address = {address}")

connected = True

start_time = time.time()
start_timestamp = timestamp()

if tcp_sock:
    seq = SEQ
    ack_seq = seq + BUFFER_SIZE
    with open(args.input, 'rb') as f:
        while connected:
            for i in range(CWND):
                data = f.read(BUFFER_SIZE-HEADER_SIZE)
                print(send_window.qsize())
                if data:
                    try:
                        ping_pong_header_p, ping_pong_header = header(tcp_sock.getsockname()[1], address[1],
                                                                      data, seq, ack_seq=ack_seq, ack=True)
                        packet = ping_pong_header_p + data
                        send_window.put((packet, address, tcp_sock))
                        ack_seq += ping_pong_header[2]
                        newthread = ServerThread(send_window, resend_window)
                        newthread.start()
                    except KeyboardInterrupt:
                        print("Keyboard Interrupt, exiting and closing connections.")
                        close(tcp_sock, address)
                        kill_threads.set()
                        break
                    except TypeError:
                        kill_threads.set()
                        break
                else:
                    print("Finished transferring")
                    close(tcp_sock, address)
                    kill_threads.set()
                    connected = False
                    break
            send_window.join()
            TRANSMISSION_ROUND += 1
            timeout_calc(calc=True)
            file_logging(f"{TRANSMISSION_ROUND} | {CWND} | {ssthresh}", stats_file_path)
            for i in range(resend_window.qsize()):  # resend packets that were triggered to be resent
                newthread = ServerThread(resend_window, resend_window)
                newthread.start()
                resend_window.join()
else:
    print("Server rejected the connection.")
    kill_threads.set()

end_time = time.time()
end_timestamp = timestamp()
total_bwidth = (PACKET_COUNT * BUFFER_SIZE) / (end_time - start_time)
print(f"Start Time: {start_timestamp}\nEnd Time: {end_timestamp}\nTotal Time: {end_time - start_time}")
print(f"Total Packets Sent (Including Retransmits): {PACKET_COUNT}")
print(f"Total Packets Lost: {TIMEOUT_COUNT}")
print(f"Total Bandwidth Achieved: {total_bwidth}")
file_logging(f"Start Time: {start_timestamp}\nEnd Time: {end_timestamp}\nTotal Time: {end_time - start_time}", final_stats_file_path)
file_logging(f"Total Packets Sent (Including Retransmits): {PACKET_COUNT}", final_stats_file_path)
file_logging(f"Total Packets Lost: {TIMEOUT_COUNT}", final_stats_file_path)
file_logging(f"Total Bandwidth Achieved: {total_bwidth}", final_stats_file_path)