from mrjob.job import MRJob   # MRJob version
class retailtab(MRJob):  #MRJob version
    def mapper(self, key, line):
        self.cache = {}
        self.max_cache_size = 100
        parts = line.split("\t")
        country = parts[7]  # fill in the number here
        quantity = parts[3] # fill in the number here
        stockcode = parts[1] # fill in the number here

        # there are num_passengers leaving the state (this is the meaning of this key,value message)
        for key in line.split():
               key = country
               value = (quantity, "Outgoing")
               self.cache[key] = {value}
               yield (key, value)

        # there are num_passengers arriving in the state (this is the meaning of this key,value message)

        for key in line.split():
               key = stockcode
               value = (quantity, "Incoming")
               self.cache[key] = {value}
               yield (key, value)

    def reducer(self, key, values):
        #key would look like Fl
        # values would like [(3, "Incoming"), (4, "Outgoing"), (6, "Incoming")]
        # you should get x=total number of incoming, y=total number of outgoing
        key = self.cache[key]
        self.cache.pop(key)
        yield (key, (x,y))

if __name__ == '__main__':
    Flight.run()   # MRJob version
