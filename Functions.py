import numpy as np
import socket
import struct
import sys
from numba import jit

month_dic = {'Jan': '01',
             'Feb': '02',
             'Mar': '03',
             'Apr': '04',
             'May': '05',
             'Jun': '06',
             'Jul': '07',
             'Aug': '08',
             'Sep': '09',
             'Oct': '10',
             'Nov': '11',
             'Dec': '12'}


class Functions:
    @staticmethod
    def fun_convert_hex_to_flags(hex_flag):
        """
        :param hex_flag: Hex decimal number representing 12-bit TCP header
        :return: flags in string
        """
        for i in range(len(hex_flag)):
            binary = bin(int(hex_flag[i], 16))[2:].zfill(12)  # header size is 12-bit
            flags = '000.'
            if binary[3] == '1':
                flags += 'NS.'
            if binary[4] == '1':
                flags += 'CWR.'
            if binary[5] == '1':
                flags += 'ECN.'
            if binary[6] == '1':
                flags += 'URG.'
            if binary[7] == '1':
                flags += 'ACK.'
            if binary[8] == '1':
                flags += 'PSH.'
            if binary[9] == '1':
                flags += 'RST.'
            if binary[10] == '1':
                flags += 'SYN.'
            if binary[11] == '1':
                flags += 'FIN.'

            hex_flag[i] = flags

        return hex_flag

    @staticmethod
    def fun_convert_timestamps(datetime_frame):
        """
        :param datetime_frame: [month & day, year & time]
        :return: date time in yy/MM/dd hh:mm:ss.sssssssss
        """
        for i in range(len(datetime_frame)):
            month_day = datetime_frame[i, 0]
            year_time = datetime_frame[i, 1]
            year = year_time[1:5]
            month = month_dic[month_day[:3]]
            day = month_day[-2:].replace(' ', '0')
            hour = year_time[6:8]
            minute = year_time[9:11]
            seconds = year_time[12:24]

            datetime_frame[i] = year+'/'+month+'/'+day+' '+hour+':'+minute+':'+seconds

        return datetime_frame

    @staticmethod
    def find_trace(array):
        ids = -np.ones((len(array),), dtype=np.int32)
        s_c = np.zeros_like(ids, dtype=str)
        current_case_id = 0

        for i in range(len(ids)):
            if ids[i] == -1:
                client = np.where(np.all(array == array[i], axis=1))[0]
                server = np.where(np.all(array == array[i, [1, 0, 3, 2]], axis=1))[0]

                ids[server] = current_case_id
                ids[client] = current_case_id
                s_c[server] = 'S'
                s_c[client] = 'C'
                current_case_id += 1

            sys.stdout.write('\rProcessed traces: {}'.format(i))
            sys.stdout.flush()

        return ids, s_c

    @staticmethod
    def fun_remove_incomplete(traces):
        for trace in traces:
            if not (('000.SYN.' in trace[0, 6] or '000.CWR.ECN.SYN.' in trace[0, 6])
                    and (any('FIN' in s for s in trace[:, 6]) or any('RST' in s for s in trace[:, 6]))):
                trace[:, 6] = 'Bad'

        return traces

    @staticmethod
    def ip2long(ip_array):
        """
        Convert an IP string to long
        """
        ip_int = np.zeros_like(ip_array, dtype=np.int)
        for i in range(len(ip_array)):
            packed_ip = socket.inet_aton(ip_array[i])
            ip_int[i] = struct.unpack("!L", packed_ip)[0]
        return ip_int
