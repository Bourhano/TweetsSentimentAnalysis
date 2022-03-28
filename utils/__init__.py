def read_keys(path="keys.txt"):
    keys = []
    with open(path) as fp:
        for _ in range(4):
            keys.append(fp.readline().strip())
    return keys
