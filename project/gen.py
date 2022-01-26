import sys
n = int(sys.argv[1])
for i in range(n):
    for j in range(i + 1, n):
        print("x{}-x{}".format(i,j))
