def read_keys():
    keys = []
    with open("keys.txt") as fp:
        for _ in range(4):
            keys.append(fp.readline().strip())
    return keys
