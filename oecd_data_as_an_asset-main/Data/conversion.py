# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 17:07:52 2023

@author: Park_M
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 12:06:10 2023

@author: Park_M
"""


###converting the xml files to parquet 

import os
import glob2
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import subprocess

pw = ""

# p = subprocess.Popen("kinit Jamgbadi_E"+pw, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
# while p.poll() is None:
#     out = p.stdout.readline()
#     print(out)
#     out = p.stderr.readline()
#     print(out)

def convert_xml_to_parquet(xml_file, parquet_folder, chunk_size=200000):
    # XML 파일 로드
    tree = ET.parse(xml_file)
    root = tree.getroot()
    print(len(root))

    # XML 데이터를 DataFrame으로 변환
    data = []
    for item in root:
        row = {}
        for child in item:
            if child.tag == 'CanonEmployer':
                # "non-struct"로 변환
                row[child.tag] = child.text
            else:
                row[child.tag] = child.text
        data.append(row)
    df = pd.DataFrame(data)

    # DataFrame을 Parquet 파일로 분할하여 저장
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        start_index = i * chunk_size
        end_index = min((i + 1) * chunk_size, len(df))
        chunk_df = df.iloc[start_index:end_index]

        # 생성될 Parquet 파일의 이름 설정
        parquet_filename = os.path.splitext(os.path.basename(xml_file))[0] + f"_{i+1}.parquet"
        parquet_filepath = os.path.join(parquet_folder, parquet_filename)

        # Parquet 파일로 변환하여 저장
        table = pa.Table.from_pandas(chunk_df)
        pq.write_table(table, parquet_filepath)

        print(f"Converted XML data to Parquet: {parquet_filepath}")

    print("Conversion completed.")

def upload_parquet_to_hadoop(parquet_file, parquet_filename, hdfs_path):
    # print(parquet_file)
    # print(hdfs_path)
    #command = f"hdfs dfs -put {parquet_file} {hdfs_path}"
    df = pd.read_parquet(parquet_file)
    df.to_parquet(f"{hdfs_path}/{parquet_filename}", index=False)

    #process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #print(process)
    #HadoopFile, std_err = process.communicate()
    #print(HadoopFile)

    # while True:
    #     output = process.stdout.readline("utf-8").decode().strip()
    #     error = process.stderr.readline("utf-8").decode().strip()

    #     if output:
    #         print(output)
    #     if error:
    #         print(error)

    #     if process.poll() is not None:
    #         break

    # if process.returncode == 0:
    #     print("Parquet 파일 업로드 성공")
    # else:
    #     print("Parquet 파일 업로드 실패")

def convert_xml_to_parquet_from_zip(zip_ref, xml_file, parquet_folder, hdfs_path, chunk_size=200000):
    extracted_xml_file = zip_ref.extract(xml_file)

    convert_xml_to_parquet(extracted_xml_file, parquet_folder, chunk_size=chunk_size)

    os.remove(extracted_xml_file)

    parquet_filename = os.path.splitext(os.path.basename(xml_file))[0] + "_1.parquet"
    parquet_filepath = os.path.join(parquet_folder, parquet_filename)
    print("uploading to hadoop")

    # Parquet 파일을 Hadoop에 업로드
    upload_parquet_to_hadoop(parquet_filepath, parquet_filename, hdfs_path)

def convert_xmls_and_upload_to_hadoop(xml_folder, hdfs_base_path, start_year, end_year, chunk_size=200000):
    parquet_folder = '\\\\oecdmain\\em_sources\\DIT_VALUEOFDATA\\UKData\\parquet'
    print("in the folder")

    for year in range(start_year, end_year + 1):
        year_folder = os.path.join(xml_folder, str(year))
        year_hdfs_path = os.path.join(hdfs_base_path, str(year))
        print("in the first for loop")

        #zip_files = glob2.glob(os.path.join(year_folder, '*.zip'))
        zip_files = glob2.glob(os.path.join(year_folder, '*.zip'))
        print(zip_files)
        for zip_file in zip_files:
            print(f"Processing ZIP file: {zip_file}")
            print("ïn the second for loop")

            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                xml_file = zip_ref.namelist()[0]

                # Parquet로 변환하여 임시 폴더에 저장 및 Hadoop에 업로드
                convert_xml_to_parquet_from_zip(zip_ref, xml_file, parquet_folder, year_hdfs_path, chunk_size=chunk_size)

            print(f"Completed processing ZIP file: {zip_file}")
            print("=" * 40)

            #Parquet 파일 개수 확인 및 삭제
            parquet_files = glob2.glob(os.path.join(parquet_folder, '*.parquet')) #glob2
            if len(parquet_files) >= 2:
                print("check length")
                oldest_files = sorted(parquet_files, key=os.path.getctime)[:2]
                for file in oldest_files:
                    os.remove(file)
                    print(f"Deleted Parquet file: {file}")
# 입력값 설정
xml_folder = 'V:\\DIT_VALUEOFDATA\\sources\\UKData\\' #'\\\\oecdmain\\em_sources\\NOVA\\BGT_FT\\UK\\'  # XML 파일이 위치한 폴더 경로
print(xml_folder)
hdfs_base_path = 'hdfs://sdsh/em_sources/_SDD/DIT_VALUEOFDATA/raw_data/UK/' #'/em_sources/_SDD/DIT_VALUEOFDATA/raw_data/UK/'
start_year = 2022
end_year = 2022
chunk_size = 1000000

#xml_file = 'UK_XML_Jobs_AddFeed_20150101_20150107.zip'
# 함수 호출
convert_xmls_and_upload_to_hadoop(xml_folder, hdfs_base_path, start_year, end_year, chunk_size=chunk_size)


# print(xml_file)
# print(parquet_folder)
# convert_xml_to_parquet(xml_file, parquet_folder, chunk_size=200000)
# upload_parquet_to_hadoop(parquet_file, hdfs_path)
# convert_xml_to_parquet_from_zip(zip_ref, xml_file, parquet_folder, hdfs_path, chunk_size=200000)
    
