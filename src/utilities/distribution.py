def block_distribution(seq, num):
    avg = len(seq) / float(num)
    out = []
    last = 0.0
    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg
    return out

def chunking(seq, chunk_size):
    out = []
    for i in range(0, len(seq), chunk_size):
        out.append(seq[i:i+chunk_size])
    return out