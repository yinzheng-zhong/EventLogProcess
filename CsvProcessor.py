import pandas as pd
from Functions import Functions
from multiprocessing import Pool
import sys
import numpy as np


class CsvProcessor:
    def __init__(self, path, num_worker):
        self.csv_path = path
        self.csv_file = None
        self.num_worker = num_worker

    def load_csv(self):
        self.csv_file = pd.read_csv(self.csv_path,
                                    names=['Date',
                                           'Timestamp',
                                           'Src_IP',
                                           'Dst_IP',
                                           'Scr_port',
                                           'Dst_port',
                                           'Flags'],
                                    error_bad_lines=False,
                                    index_col=False,
                                    low_memory=False)

        print('Loaded')

    def save_csv(self):
        self.csv_file.to_csv(self.csv_path[:-4]+'_modified.csv', index=False)

    def use_small_portion_200k(self):
        self.csv_file = self.csv_file.iloc[:200000, :]

    def remove_invalid_rows(self):
        self.csv_file.dropna(inplace=True)
        self.csv_file.reset_index(drop=True, inplace=True)
        try:
            self.csv_file = self.csv_file[self.csv_file['Scr_port'].map(len) <= 5]
        except TypeError:
            print(TypeError)
        print('Dropped rows with NaN')

    def convert_hex_to_flags(self):
        print('\nStart converting Hex decimal to flags.')
        pool = Pool(self.num_worker)
        array = self.csv_file['Flags'].values
        sub_arrays = np.array_split(array, self.num_worker)

        # use list to keep values temporarily instead of directly assigning them to pd series is much faster
        flag_series = []

        for chunk in pool.imap(Functions.fun_convert_hex_to_flags, sub_arrays):
            flag_series.append(chunk)

        self.csv_file['Flags'] = np.concatenate(flag_series, axis=0)
        print('Converted Hex decimal to flags.')

    def convert_timestamp(self):
        print('\nStart converting datetime to standard date time.')

        pool = Pool(self.num_worker)
        array = self.csv_file.loc[:, ['Date', 'Timestamp']].values
        # more efficient to break data into chunks then do the loop in function
        sub_array = np.array_split(array, self.num_worker)

        time_series = []

        for chunk in pool.imap(Functions.fun_convert_timestamps, sub_array):
            time_series.append(chunk)

        self.csv_file['Timestamp'] = np.concatenate(time_series, axis=0)
        self.csv_file.drop('Date', axis=1, inplace=True)
        print('Converted datetime to standard date time.')

    def construct_traces(self):
        print('\nStart trace construction.')
        self.csv_file.insert(0, 'Case_ID', 0)
        self.csv_file.insert(7, 'S/C', '#')

        # take out array for better performance
        array = self.csv_file.loc[:, ['Src_IP', 'Dst_IP', 'Scr_port', 'Dst_port']].values

        src = Functions.ip2long(array[:, 0])
        dst = Functions.ip2long(array[:, 1])

        array[:, 0] = src
        array[:, 1] = dst
        array = array.astype(np.int32)

        ids, s_c = Functions.find_trace(array)

        self.csv_file['Case_ID'] = ids
        self.csv_file['S/C'] = s_c
        print('\nConstructed traces.')

    def remove_incomplete_flows(self):
        print('\nStarted removing incomplete flows.')
        pool = Pool(self.num_worker)

        num_cases = np.max(self.csv_file['Case_ID']) + 1
        print('\tFound number of cases.')
        case_ids = range(num_cases)
        cases = []
        for i in case_ids:
            cases.append(self.csv_file.loc[self.csv_file['Case_ID'] == i].values)
        print('\tSeparated cases.')

        sub_cases = []
        k, m = divmod(num_cases, self.num_worker)
        for i in range(self.num_worker):
            head = i * k
            tail = head + k

            if i == self.num_worker - 1:
                tail = num_cases

            sub_cases.append(cases[head:tail])

        processed = []

        print('\tBuilt subsets.')

        for chunk in pool.imap(Functions.fun_remove_incomplete, sub_cases):
            processed.append(np.concatenate(chunk, axis=0))

        processed = np.concatenate(processed, axis=0)
        self.csv_file.loc[:, :] = processed
        self.csv_file = self.csv_file[self.csv_file['Flags'] != 'Bad']

        print('Removed incomplete flows.')
