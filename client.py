#	client 
#	Implemented by Zhijing@Mar. 7
import json
import socket
import datetime
import sys
import time


CONFIG = json.load(open('config.json'))

client_id = sys.argv[1]

def Request(port, message):
    c = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    host = ''
    addr = (host, port)
    sent = c.sendto(message, addr)
    # !!! TODO: change recvfrom to a global wait and print
    # time.sleep(2)
    # data, server = c.recvfrom(4096)
    # c.close()

def RequestTicket(port, buy_num, request_id):
    message = ('BUY:"{client_id}",{request_id},' +
               '{ticket_count}').format(
                           client_id=client_id,
                           request_id=request_id,
                           ticket_count=buy_num)
    Request(port, message)

def RequestShow(port):
    Request(port, 'SHOW:')

def RequestChange(port):
    # TODO: add functionality for config change
    Request(port, 'CHANGE:XXXX')

def Interface_cmd():
    choice = True
    request_id = 0
    while choice:
        datacenter = CONFIG['datacenters']
        datacenter_list = []
        for i in range(1, len(datacenter)+1):
            datacenter_list.append(datacenter[str(i)]['port'])
            print('datacenter: '+ str(datacenter[str(i)]['port']))

        cmd = raw_input('Please choose a server to connect... or Press N to exit...\t')
        if cmd == 'N':
            choice = False
            break
        else:
            server_selected = int(cmd)
            if server_selected not in datacenter_list:
                print('Invalid input')
                continue

            command = raw_input('Command: buy {numberOfTicket} / show / change {param1, param2}?\t')

            if command.startswith('buy'):
                RequestTicket(server_selected, int(command.lstrip('buy ')), request_id)
                request_id += 1
            elif command.startswith('show'):
                RequestShow(server_selected)
            elif command.startswith('change'):
                RequestChange(server_selected, command.lstrip('change '))



def main():
    print("\n*********************************\n")
    print("Welcome to SANDLAB Ticket Office!")
    print("The current time is " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("\n*********************************\n")
    Interface_cmd()
    exit()

if __name__ == "__main__":
    main()

