#! python3
# sender_solano.py
# usage: python3 sender_solano.py --dest_ip XXXX.XXXX.XXXX.XXXX --dest_port YYYY --input input.txt

import socket
import argparse
import zlib
import struct
import random
import time
import os
from datetime import datetime


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
TIMEOUT_INTERVAL = 1
# the total number of timeouts for this session
TIMEOUT_COUNT = 0


def timestamp():
    timestamp = time.time()
    return datetime.utcfromtimestamp(timestamp).strftime('%Y%m%d%H%M%S')


def file_logging(msg):
    write_file = open(log_file_path, 'a')
    write_file.write(msg + '\n')
    write_file.close()
    print(msg)


log_file = f"sender_solano_{timestamp()}.log"
log_file_path = os.path.abspath(os.path.join('.', log_file))


# calculate a new timeout interval
def timeout_calc(sec):
    global TIMEOUT_INTERVAL
    global TIMEOUT_COUNT
    TIMEOUT_COUNT += 1
    t = .5
    print(f"TIMEOUT INTERVAL = {TIMEOUT_INTERVAL}\n TIMEOUT COUNT = {TIMEOUT_COUNT}")
    if TIMEOUT_COUNT == 1:
        TIMEOUT_INTERVAL = sec
    else:
        TIMEOUT_INTERVAL = 1-t*TIMEOUT_INTERVAL + t*sec


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
    s.sendto(packet, address)
    header = packet[:27]
    header = unpack_header(header)
    file_logging(f"{header[0]} | {header[1]} | {msg_type(header)} | {header[2]} | {timestamp()}")


def send_and_wait_respond(s, packet, address, re_sock):
    for i in range(0, 3):
        re_sock.settimeout(TIMEOUT_INTERVAL)
        t1 = time.time()
        try:
            send(s, packet, address)
            s_header = packet[:27]
            s_header = unpack_header(s_header)
            r_header, r_data, r_address = receive(re_sock)
            if r_header[5] == s_header[5] and not r_header[8]:  # data not received by receiver, it requested the same packet
                return send_and_wait_respond(s, packet, address, re_sock)
            else:
                return r_header, r_data, r_address
        except KeyboardInterrupt:
            close(re_sock, address)
            break
        except:  # timed out try sending the same packet again
            print(f"Timed out {i+1} times.")
            t2 = time.time()
            timeout_calc(t2-t1)
    print("timed out, giving up")
    close(re_sock, address)


def receive(rec_sock, buffer=BUFFER_SIZE):
    r, r_address = rec_sock.recvfrom(buffer)
    r_header = r[:27]
    r_header = unpack_header(r_header)
    data = r[27:]
    file_logging(f"{r_header[0]} | {r_header[1]} | {msg_type(r_header)} | {r_header[2]} | {timestamp()}")
    return r_header, data, r_address



def header(sender_port, receiver_port, data, seq, ack_seq=0, ack=False, syn=False, fin=False):
    # create the header ('struct format', 'sender port', 'receiver port', 'win',
    # 'checksum', 'seq', 'ack seq', 'ack', 'syn', 'fin')
    win = len(data)+27
    checksum = checksum_calc(data)
    if ack_seq == 0:
        ack_seq = seq
    header_packed = struct.pack("!IIIIII???", sender_port, receiver_port, win, checksum, seq,
                                ack_seq, ack, syn, fin)
    return header_packed, unpack_header(header_packed)


def unpack_header(header):
    return struct.unpack("!IIIIII???", header)


def msg_type(header):
    if header[2] > 27:
        return "DATA"
    elif header[6] and header[7]:
        return "ACK/SYN"
    elif header[6]:
        return "ACK"
    elif header[7]:
        return "SYN"
    elif header[8]:
        return "FIN"


tcp_sock, address = connect(host, port)
print(f"handhsake complete, server address = {address}")

if tcp_sock:
    seq = SEQ
    ack_seq = seq + BUFFER_SIZE
    with open(args.input, 'rb') as f:
        while True:
            data = f.read(BUFFER_SIZE-27)
            if not data:
                print("Finished transferring")
                close(tcp_sock, address)
                break
            else:
                try:
                    ping_pong_header_p, ping_pong_header = header(tcp_sock.getsockname()[1], address[1],
                                                                  data, seq, ack_seq=ack_seq, ack=True)
                    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                        r_header, r_data, r_address = send_and_wait_respond(s, ping_pong_header_p + data,
                                                                            address, tcp_sock)
                    r_data = r_data.decode()
                    ack_seq += ping_pong_header[2]
                except KeyboardInterrupt:
                    print("Keyboard Interrupt, exiting and closing connections.")
                    close(tcp_sock, address)
                    break
                except TypeError:
                    break
else:
    print("Server rejected the connection.")
