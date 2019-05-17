from CsvProcessor import CsvProcessor
import time
import pandas as pd
import numpy as np


class Normal:
    def __init__(self, paths, path_out, num_cpu):
        self.paths = paths
        self.path_out = path_out
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
            cp.remove_incomplete_flows()
            print(now - time.time())
            cp.save_csv()

    def concatenate_all(self, subset=False, subset_size=20000):
        data = []

        previous_max = 0
        for i in range(len(self.paths)):
            data.append(pd.read_csv(self.paths[i][:-4]+'_modified.csv',
                                    index_col=False,
                                    low_memory=False))

            new_id = data[i]['Case_ID'].values + previous_max
            data[i]['Case_ID'] = new_id
            previous_max = np.max(new_id)

        data = np.concatenate(data)

        dataset = pd.DataFrame(data=data,
                               columns=['Case_ID',
                                        'Timestamp',
                                        'Src_IP',
                                        'Dst_IP',
                                        'Scr_port',
                                        'Dst_port',
                                        'Flags',
                                        'S/C'])

        if subset is True:
            np.random.seed(3)
            sub_set_ids = np.random.choice(previous_max, subset_size, replace=False)
            dataset = dataset[dataset['Case_ID'].isin(sub_set_ids)]

        dataset.to_csv(self.path_out, index=False)