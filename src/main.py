from utilities.script import Script
from utilities.network import ClientConnectionManager, ServerConnectionManager
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from time import sleep

def test_scripting():
    file = open("../res/test.txt", "r")
    script = Script(file.read())
    file.close()

    script.get("map")()
    script.get("reduce")()


def server(args):
    connection_manager = ServerConnectionManager()

    # first we wait until we have a satisfactory amount of workers
    while len(connection_manager.get_clients()) < args.workers: pass

    # get the clients and start distributing the work
    clients = connection_manager.get_clients()
    while True:
        message = connection_manager.get_next_message(clients)
        if message is not None:
            print(message)
            break
        sleep(1)
    connection_manager.close()

def client(args):
    connection_manager = ClientConnectionManager()
    connection_manager.send_message('foo')
    connection_manager.close()

def main():
    parser = ArgumentParser(description='Do a map reduce on a dataset.', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-i', '--index', type=int, required=True, help='What is the index of the instance (0 is controller)')
    parser.add_argument('-w', '--workers', type=int, required=True, help='What is the minimum amount of workers required for execution')
    args = parser.parse_args()

    if args.index == 0:
        server(args)
    else:
        client(args)
        test_scripting()

if __name__ == "__main__":
    main()