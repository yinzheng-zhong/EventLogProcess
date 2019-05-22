from CsvProcessor import CsvProcessor
import time


class Anomalous:
    def __init__(self, paths, num_cpu):
        self.paths = paths
        self.num_cpu = num_cpu

    def process_individual_data(self):
        for i in range(len(self.paths)):
            cp = CsvProcessor(self.paths[i], self.num_cpu)
            cp.load_csv()
            #cp.use_small_portion_200k()
            now = time.time()
            cp.remove_invalid_rows()
            cp.convert_hex_to_flags()
            cp.convert_timestamp()
            cp.construct_traces()
            print(now - time.time())
            cp.save_csv()
