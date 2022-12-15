from mrjob.job import MRJob
class FBCount(MRJob):
    #do not need to split line into two parts
    def mapper(self , key , line):
         (left , right) = line.split(" ")
         if int(right) > 500:
             yield (left , right)

    # the left variable is not used so it can be removed
    # can use sum directly on key
    def reducer(self , key , values):
         myval = sum(key)
         if myval > 2:
            yield (key, myval +1)

if __name__ == "__main__":
    FBCount.run()
