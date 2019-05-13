if __name__ == '__main__':
    import rados
    from utils.oss.pyrados import write_part_tasks, read_part_tasks


    ceph_conffile = '/etc/ceph/ceph.conf'
    ceph_keyring = '/etc/ceph/ceph.client.admin.keyring'
    cluster = rados.Rados(conffile=ceph_conffile, conf=dict(keyring=ceph_keyring))
    cluster.connect()
    print(f"\nCluster ID: {cluster.get_fsid()}")
    print("\n\nCluster Statistics")
    print("==================")
    cluster_stats = cluster.get_cluster_stats()
    for key, value in cluster_stats.items():
        print(key, end=',')
        print(value)

    print('cluster status:')
    try:
        state = cluster.state
        print(state)
    except rados.RadosStateError as e:
        pass


    # print("\nAvailable Pools")
    # print("----------------")
    # pools = cluster.list_pools()
    # for pool in pools:
    #     print(pool)
    #
    ioctx = cluster.open_ioctx('p0')

    # print('pool_stats')
    # print("----------------")
    # pool_stats = ioctx.get_stats()
    # print(pool_stats)

    key = '00000001'
    offset = 2147483648
    # r = ioctx.write(key, b'1', offset=offset)
    # print('rados obj stats')
    # print("----------------")
    # obj_stats = ioctx.stat(key)
    # print(obj_stats)
    # try:
    #     context = ioctx.read(key, 10, offset-5)
    #     print('read obj:', context)
    # except rados.Error as e:
    #     pass

    try:
        ok = ioctx.remove_object(key)
        print('remove_obj:', ok)
    except rados.Error as e:
        pass


    r = ioctx.write(key, b'test', offset=0)
    print('rados obj stats')
    print("----------------")
    obj_stats = ioctx.stat(key)
    print(obj_stats)

    try:
        r = ioctx.write(key, b'', offset=offset)
    except rados.OSError as e:
        code = e.errno
        pass

    print('rados obj stats')
    print("----------------")
    obj_stats = ioctx.stat(key)
    print(obj_stats)

    context = ioctx.read(key, 20, offset-5)
    print('read obj:', context)

    print("\nClosing the ioctx.")
    ioctx.close()

    print("\nClosing the cluster.")
    cluster.shutdown()

