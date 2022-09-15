from mrjob.job import MRJob   # MRJob version

# Change the class name!!
class WordCount(MRJob):  #MRJob version
    def mapper(self, key, line):
        wordCount=dict()
        words = line.split()
        for w in words:
            length = w.length
            if length == dict.get(length):
                dict[length] = dict.get(length)+1
            else:
                dict[length] = 1
        print(dict)
            #check if length of word exists
            #if yes add one
            #else create and set as 1
            

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    WordCount.run()   # MRJob version
