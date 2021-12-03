

def main():
    i = (227 % 27)
    if ((i % 4) == 1):
        print('test12 begins')
        print('test12 ends')
    elif ((i % 4) == 2):
        print('test34 begins')
        print('test34 ends')
    elif ((i % 4) == 3):
        print('test56 begins')
        print('test56 ends')
    else:
        print('test12 begins')
        print('test12 ends')
    x = []
    x.append(1)
    virtual_return_register_0 = 1
    x.append(1)
    virtual_return_register_1 = 1
    if (virtual_return_register_0 > 1):
        print(1)
    elif (virtual_return_register_1 > 2):
        print(2)
    elif (len(x) > 1):
        print(3)

main()