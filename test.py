from operator import itemgetter
if __name__ == "__main__":
    a=[('a','b','a'),('a','c',''),('a','b','')]
    a.sort(key=itemgetter(0,1,2))
    print(a)



