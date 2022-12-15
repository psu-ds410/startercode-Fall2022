from mrjob.job import MRJob   # MRJob version
class Flight(MRJob):  #MRJob version
    def mapper(self, key, line):
        self.cache = {}
        self.max_cache_size = 100
        parts = line.split("\t")
        origin_airport = parts[3]  # fill in the number here
        destination_airport = parts[5] # fill in the number here
        num_passengers = parts[7] # fill in the number here

        # there are num_passengers leaving the state (this is the meaning of this key,value message)
        for key in line.split():
               key = origin_airport
               value = (num_passenger, "Outgoing")
               self.cache[key] = {value}
               yield (key, value)

        # there are num_passengers arriving in the state (this is the meaning of this key,value message)

        for key in line.split():
               key = destination_airport
               value = (num_passengers, "Incoming")
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

