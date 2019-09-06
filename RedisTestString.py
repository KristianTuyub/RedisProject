"""
Author: Christian Tuyub
Subject: Manejo de Datos No Estructurados
Date: 2019-09-05
"""

# step 1: import the redis-py client package
import redis

# step 2: define our connection information for Redis
# I replaced default 6379 port with 6380
redis_host = "localhost"
redis_port = 6380
redis_password = ""


def connection(parameter_function):
    """Decorator Function to Make a Connection per every process"""

    def interior_function(*args):
        """Interior Processing; establishes a connection to redis server"""
        # do connection
        # step 3: create the Redis Connection object
        try:
            print("Connecting to redis...")
            # The decode_responses flag here directs the client to convert the responses from Redis into Python strings
            # using the default encoding utf-8.  This is client specific.
            redis_env = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password,
                                          decode_responses=True)

        except Exception as exception:
            print(exception)

        print("Connection Successful!")

        # Call to method passed by parameter; redis_env is passed to required function to do ops like get, set, etc
        parameter_function(redis_env)
        # do additional things, if wanted

    return interior_function


def welcome_user_input():
    print("""Enter a number according to the structure type you want to work: 
             0: STRING
             1: LIST
             2: SET
             3: HASH
             4: ZSET
    """)
    user_input_str = input(" ")

    analyze_and_execute(user_input_str)


@connection
def string_operations(redis_env):
    print("""Enter a number according to the operation type you want to do (STRING STRUCTURE): 
             0: GET
             1: SET
             2: DEL\n
    """)

    string_command = input("")

    if string_command == "0":
        str_to_operate = input("Enter the key to GET its value\n")
        msg = redis_env.get(str_to_operate)
        print("There's no STRING with that key :c " if msg is None else msg)
    elif string_command == "1":
        str_usr = input("Enter a new string you want to SET\n")
        str_value_usr = input("Enter its value\n")
        msg = redis_env.set(str_usr, str_value_usr)
        print("success!! " if msg == 1 else msg)
    elif string_command == "2":
        str_to_operate = input("Enter the String you want to DELETE\n")
        msg = redis_env.delete(str_to_operate)
        print("success!! " if msg == 1 else msg)
    else:
        print("Select the correct operation, please\n")


@connection
def list_operation(redis_env):
    print("""Enter a number according to the operation type you want to do (LIST STRUCTURE): 
             0: RPUSH
             1: LRANGE
             2: LINDEX
             3: LPOP\n
    """)

    list_command = input("")

    if list_command == "0":
        list_to_operate = input("Enter the List's Name\n")
        value_to_add = input("Enter the value you want to RPUSH to {0}".format(list_to_operate))
        msg = redis_env.rpush(list_to_operate, value_to_add)
        print("Added {0} to {1}. Current length: {2}".format(value_to_add, list_to_operate, msg))
    elif list_command == "1":
        list_to_operate = input("Enter the List's Name\n")
        range1 = input("Enter the start point range of elements to retrieve of the list {0} ".format(list_to_operate))
        range2 = input("Enter the end point range of elements to retrieve of the list {0} ".format(list_to_operate))
        msg = redis_env.lrange(list_to_operate, range1, range2)
        print("There's no list with that name, or there're no elements in the range you provided" if not msg else msg)
    elif list_command == "2":
        list_to_operate = input("Enter the List's Name you want to retrieve data from\n")
        index_list = input("Enter the index of the desired value ")
        msg = redis_env.lindex(list_to_operate, index_list)
        print("Element not found, or list doesn't exists" if msg is None
              else "Found value: {0} at index {1}".format(msg, index_list))
    elif list_command == "3":
        list_to_operate = input("Enter the List's Name you want to LPOP data\n")
        msg = redis_env.lpop(list_to_operate)
        print("List doesn't exists or is empty" if msg is None
              else "Element {0} popped from {1}".format(msg, list_to_operate))
    else:
        print("Select the correct operation, please\n")


@connection
def set_operations(redis_env):
    print("""Enter a number according to the operation type you want to do (SET STRUCTURE): 
             0: SADD
             1: SMEMBERS
             2: SISMEMBER
             3: SREM\n
    """)

    set_command = input("")

    if set_command == "0":
        set_to_operate = input("Enter the SET's name\n")
        item_to_add = input("Enter the item to add to {0}".format(set_to_operate))
        msg = redis_env.sadd(set_to_operate, item_to_add)
        print("success!! {0} added to {1}".format(item_to_add, set_to_operate) if msg == 1
              else "error, code: {0}".format(msg))
    elif set_command == "1":
        set_to_operate = input("Enter the SET's name you want to retrieve data from\n")
        msg = redis_env.smembers(set_to_operate)
        print("error, no members or set doesn't exist" if not msg
              else "{0} elements: {1}".format(set_to_operate, msg))
    elif set_command == "2":
        set_to_operate = input("Enter the SET's name you want to operate over\n")
        item_to_evaluate = input("Enter item to check if {0} has it".format(set_to_operate))
        msg = redis_env.sismember(set_to_operate, item_to_evaluate)
        print("{0} is a member of {1}".format(item_to_evaluate, set_to_operate) if msg == 1
              else "{0} is not a member or {1} doesn't exist".format(item_to_evaluate, set_to_operate))
    elif set_command == "3":
        set_to_operate = input("Enter the SET's name you want to operate over\n")
        item_to_remove = input("Enter item to remove from {0}".format(set_to_operate))
        msg = redis_env.srem(set_to_operate, item_to_remove)
        print("removed!!" if msg == 1 else "error, item or set doesn't exist. Code: {0}".format(msg))
    else:
        print("Select the correct operation, please\n")


@connection
def hash_operations(redis_env):
    print("""Enter a number according to the operation type you want to do (HASH STRUCTURE): 
             0: HSET
             1: HGET
             2: HGETALL
             3: HDEL\n
    """)

    hash_command = input("")

    if hash_command == "0":
        hash_to_operate = input("Enter the Hash Name\n")
        hash_key = input("Enter hash key")
        hash_value = input("Enter element to add to {0}".format(hash_key))
        msg = redis_env.hset(hash_to_operate, hash_key, hash_value)
        print("success!!" if msg == 1 else "error. Code: {0}".format(msg))
    elif hash_command == "1":
        hash_to_operate = input("Enter the Hash Name\n")
        hash_key = input("Enter hash key")
        msg = redis_env.hget(hash_to_operate, hash_key)
        print("value {0}".format(msg))
    elif hash_command == "2":
        hash_to_operate = input("Enter the Hash Name\n")
        msg = redis_env.hgetall(hash_to_operate)
        print("elements of {0}: {1}".format(hash_to_operate, msg))
    elif hash_command == "3":
        hash_to_operate = input("Enter the Hash Name\n")
        hash_key = input("Enter hash key")
        msg = redis_env.hdel(hash_to_operate, hash_key)
        print("success!!" if msg == 1 else "error. Code: {0}".format(msg))
    else:
        print("Select the correct operation, please\n")


@connection
def zset_operations(redis_env):
    print("""Enter a number according to the operation type you want to do (ZSET STRUCTURE): 
             0: ZADD
             1: ZRANGE
             2: ZREM\n
    """)

    zset_command = input("")

    if zset_command == "0":
        zset_key = input("Enter the ZSET Key\n")
        zset_sub_key = input("Enter ZSET Sub-key ")
        zset_value = input("Enter ZSET Value to add to {0}".format(zset_sub_key))
        msg = redis_env.zadd(zset_key, {zset_sub_key: zset_value})
        print("success!!" if msg == 1 else "error, code: {0}".format(msg))
    elif zset_command == "1":
        zset_key = input("Enter the ZSET Key\n")
        zset_range_start_point = input("enter start range point: ")
        zset_range_end_point = input("enter end range point: ")
        msg = redis_env.zrange(zset_key, zset_range_start_point, zset_range_end_point)
        print("error, no members or set doesn't exist" if not msg else msg)
    elif zset_command == "2":
        zset_key = input("Enter the ZSET Key\n")
        zset_sub_key = input("Enter ZSET Sub-key ")
        zset_value_to_remove = input("Enter ZSET Value to remove from {0} ".format(zset_sub_key))
        msg = redis_env.zrem(zset_key, zset_sub_key, zset_value_to_remove)
        print("success!!" if msg == 1 else "error, code: {0}".format(msg))
    else:
        print("Select the correct operation, please\n")


def analyze_and_execute(operation):
    if operation == "0":
        string_operations()
    elif operation == "1":
        list_operation()
    elif operation == "2":
        set_operations()
    elif operation == "3":
        hash_operations()
    elif operation == "4":
        zset_operations()
    else:
        print("Select the correct type structure, please")


def start_program():
    flag = True
    while flag:
        try:
            welcome_user_input()
            welcome_user_input_str = input("Want to continue? (y / n) \n")

            if welcome_user_input_str == "y":
                flag = True
            elif welcome_user_input_str == "n":
                flag = False
            elif welcome_user_input_str != "y" or welcome_user_input_str != "n":
                print("Write y or n, please.\n")
                flag = True

        except ValueError:
            print("Sorry, I didn't understand that.")
            # better try again... Return to the start of the loop
            continue


if __name__ == '__main__':
    start_program()
