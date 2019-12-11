from utilities.script import Script
from utilities.network import ClientConnectionManager, ServerConnectionManager
from utilities.distribution import block_distribution, chunking
from utilities.file_utils import read_file_contents
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from multiprocessing import Pool, cpu_count
import itertools
import traceback

DEAD_PILL = 0
SCRIPTS = 1
MAP = 2
REDUCE = 3
FINISHED = 4
MAP_REDUCE_SINGLE_PHASE = 5

# these methods are utilities for the multiprocessing module
def mp_init_worker(script_source):
    global script
    script = Script(script_source)

def mp_get_script():
    global script
    return script


# helpers for the map and reduce methods
# if the script argument is none then we assume multiprocessing and
# search for a previously made global instance in this process
def map_helper(data, script=None):
    if script is None:
        script = mp_get_script()

    # perform a map operation on the data object
    return script.get('map')(data)

def reduce_helper(data, script=None):
    if script is None:
        script = mp_get_script()

    # perform the reduce operation on the dataset
    curr_result = script.get("reduce_start_value")()
    for item in data:
        curr_result = script.get("reduce")(item, curr_result)
    return curr_result



# this executes a distinct phase of the map reduce system
def server_do_phase(connection_manager, clients, heartbeat, phase, work_list):
    # next we execute the map method on the data set
    finished = { c.cid: True for c in clients}
    results = []
    while True:
        # first we make sure that every client is still alive
        for client in clients[:]:
            if client.seconds_since_last_heartbeat() > heartbeat:
                # this client is probably not alive anymore, redistribute the work
                print(f"Client {client.cid} died")

                clients.remove(client)
                finished.pop(client.cid, None)

                work = client.get_user_data()
                if work is not None:
                    work_list.append(work)
                client.set_user_data(None)

                if len(clients) == 0:
                    raise RuntimeError("All clients died")

        # check for inbound messages
        message = connection_manager.get_next_message(clients)
        if message is not None:
            client, message = message
            results.append(message['result'])
            finished[client.cid] = True
            client.set_user_data(None)

        # check whether we can send new work to any client
        for client in clients:
            if finished[client.cid] and len(work_list) > 0:
                finished[client.cid] = False
                work = work_list.pop()
                client.set_user_data(work)
                connection_manager.send_message(client, {
                    'type': phase,
                    'work': work
                })

        # if all clients have responded then we're done
        if all(finished.values()) and len(work_list) == 0:
            break

    return results

def server(args):
    workers = args.world - 1
    connection_manager = ServerConnectionManager(args.network_host,
                                                 args.network_port,
                                                 heartbeat_max_interval=args.heartbeat,
                                                 max_connections=workers)

    # get the clients and start distributing the work
    clients = connection_manager.get_clients()

    try:
        # first we broadcast the script
        script_source = read_file_contents(args.script_source)
        script = Script(script_source)
        assert script.has("get_dataset"), "get_dataset function is mandatory"
        assert script.has("process_result"), "process_result function is mandatory"
        assert script.has("map"), "map function is mandatory"
        assert script.has("reduce"), "reduce function is mandatory"
        assert script.has("reduce_start_value"), "reduce_start_value function is mandatory"
        connection_manager.broadcast_message(clients, {
            'type': SCRIPTS,
            'script': script_source
        })

        # Perform the map phase
        data = script.get("get_dataset")()
        chunks = workers - 1 if workers > 1 else workers
        work = block_distribution(data, chunks)

        if args.single_phase:
            reduce_results = server_do_phase(connection_manager, clients, args.heartbeat, MAP_REDUCE_SINGLE_PHASE, work)
            print("Finished map and reduce phase")
        else:
            map_results = server_do_phase(connection_manager, clients, args.heartbeat, MAP, work)
            print("Finished map phase")

            # Perform the reduce phase
            reduce_results = server_do_phase(connection_manager, clients, args.heartbeat, REDUCE, map_results)
            print("Finished the reduce phase")

        # process the final result
        final_result = reduce_helper(reduce_results, script)
        script.get("process_result")(final_result)
    except:
        traceback.print_exc()
        print("Server halted")

    connection_manager.broadcast_message(clients, {'type': DEAD_PILL})
    connection_manager.close()


def client(args):
    number_of_cpu = cpu_count()
    connection_manager = ClientConnectionManager(args.network_host, args.network_port)
    running = True
    executor_pool = Pool(number_of_cpu)
    current_script = None

    try:
        if args.index == args.debug_crash_node:
            raise RuntimeError("Debug crashing this node :D")

        while running:
            msg = connection_manager.get_next_message_blocking()
            if msg['type'] == DEAD_PILL:
                running = False

            elif msg['type'] == SCRIPTS:
                executor_pool.close()
                executor_pool.join()

                current_script = Script(msg['script'])
                executor_pool = Pool(number_of_cpu, initializer=mp_init_worker, initargs=(msg['script'], ))

            elif msg['type'] == MAP:
                work = msg['work']
                mapped_data = executor_pool.map(map_helper, work)
                connection_manager.send_message({
                    'type': FINISHED,
                    'result': mapped_data
                })

            elif msg['type'] == REDUCE:
                work = block_distribution(msg['work'], number_of_cpu)
                reduced_data = executor_pool.map(reduce_helper, work, chunksize=1)
                reduced_data = reduce_helper(reduced_data, current_script)
                connection_manager.send_message({
                    'type': FINISHED,
                    'result': reduced_data
                })

            elif msg['type'] == MAP_REDUCE_SINGLE_PHASE:
                work = msg['work']
                mapped_data = executor_pool.map(map_helper, work)
                work = block_distribution(mapped_data, number_of_cpu)
                reduced_data = executor_pool.map(reduce_helper, work, chunksize=1)
                reduced_data = reduce_helper(reduced_data, current_script)
                connection_manager.send_message({
                    'type': FINISHED,
                    'result': reduced_data
                })

    except:
        traceback.print_exc()
        print("Client halted")

    executor_pool.close()
    executor_pool.join()
    connection_manager.close()


def main():
    parser = ArgumentParser(description='Do a map reduce on a dataset.', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-i', '--index', type=int, required=True, help='What is the index of the instance (0 is server)')
    parser.add_argument('-w', '--world', type=int, required=True, help='What is the size of the world')
    parser.add_argument('-hb', '--heartbeat', type=float, default=4.0, help="Heartbeat interval")
    parser.add_argument('-nh', '--network_host', type=str, default='127.0.0.1', help="The host address")
    parser.add_argument('-np', '--network_port', type=int, default=1234, help="The port to connect to")
    parser.add_argument('-ss', '--script_source', type=str, default="../res/test.txt", help="Where is the script source")
    parser.add_argument('-spmr', '--single_phase', type=bool, default=False, help="Whether to do a single phase map reduce")
    parser.add_argument('-dbgcn', '--debug_crash_node', type=int, default=-1, help="The node to crash to test how the system handles failing nodes")
    args = parser.parse_args()
    assert args.world > 1, "There should be at least 1 worker"

    if args.index == 0:
        server(args)
    else:
        client(args)


if __name__ == "__main__":
    main()
