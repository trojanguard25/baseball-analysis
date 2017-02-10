
class AdaBoost(self):

    def Init(self, m, key):
        self._m = m
        self._key = key
        
        self._w = []
        
    def FindBestN(self, num):
        if (num > len(self._m)):
            num = len(self._m)
            
        results = []
        for x, picks in self._m.iteritems():
            for pick in picks:
                self._w.append(1.0)
            break
            
        W = len(self._w)    
            
        for i in range(0, num):
            best_W_e = 10000.0
            selected_id = ''
            for id, picks in self._m.iteritems():
                W_e = 0
                i = 0
                for pick in picks:
                    W_e += pick * self._w[i]
                    i += 1
                    
                if W_e < best:
                    best_W_e = W_e
                    selected_id = id
                    
            
            e_m = best_W_e / W
            weight = 0.5 * ln ((1.0-e_m)/(e_m))
            result = { selected_id : weight }
            results.append(result)
            
            # update weights
            i = 0
            for pick in self._m[selected_id]:
                if pick == 0:
                    self._w[i] = self._w[i] * e ^ (-1 * weight)
                else:
                    self._w[i] = self._w[i] * e ^ (weight)
                i += 1
                    
            W = 0        
            for w in self._w:
                W += w

            # removed id from self._m
            
             