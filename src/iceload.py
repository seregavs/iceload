from pyspark.sql import SparkSession
import spark_const
import datetime
import os
import yaml
import json
import boto3
# import re
from pydantic import TypeAdapter
from typing import List


class IceLoad:
    """Класс для загрузки данных BW в таблицы iceberg
        icebergtbl_props - имя файла с параметрами хранения iceberg-таблиц. Нужно для CREATE TABLE
        srcfile_log_name - имя файла, в котором сохраняются обработанные файлы данных. Нужно для контроля повторной обработки файлов
        остальные переменные - компоненты SQL-запросов для логики автогенерации SQL
    """
    icebergtbl_props = "/home/alpine/iceload/src/icebergtbl_props.yaml"  # для локальных хранений настроечных файлов (не в s3)
    request_fields_create = 'reqtsn STRING NOT NULL, datapakid STRING NOT NULL, record INT NOT NULL'
    request_fields_max = 'max(reqtsn || datapakid || to_char(record))'
    request_fields = 'reqtsn, datapakid, record'
    request_fields_join = 't1.reqtsn = t2.reqtsn AND t1.datapakid = t2.datapakid AND t1.record = t2.record'
    k_table_prefix1 = "SELECT substr(mkey,1,23) as reqtsn, substr(mkey,24,6) as datapakid, cast(substr(mkey,30,8) as integer) as record"
    k_table_prefix2 = "FROM (SELECT max(reqtsn || datapakid || record) as mkey "
    srcfile_log_name = 'srcfile'
    bucket_default = 'stg-bi-1'
    srcbucket = ''
    database_default = 'db'
    safe_dml_default = False
    metadata_source = 'local'  # local | s3
    # log_time_indent = ' 0000s. '

    def __init__(self, md: str, srcfiles: list,
                 actions: list, metadata: str):
        """Конструктор класса для загрузки данных в формат iceberg

        Args:
            md (str): имя загружаемой из BW сущности (напр, техимя ADSO или инфообъекта)
            srcfiles (list): Список файлов данных для загрузки (полные пути к файлам)
            actions (list): Список действий с данными
            metadata (str, optional): Относительный путь к файлу с метаданными - описание загрузки'.
        """
        # Настройки для подключения к S3
        self.YC_ACCESS_KEY_ID = os.environ.get("YC_ACCESS_KEY_ID") if os.environ.get("YC_ACCESS_KEY_ID") else ''
        self.YC_SECRET_ACCESS_KEY = os.environ.get("YC_SECRET_ACCESS_KEY") if os.environ.get("YC_SECRET_ACCESS_KEY") else ''
        self.YC_REGION_NAME = os.environ.get("YC_REGION_NAME") if os.environ.get("YC_REGION_NAME") else ''
        self.YC_ENDPOINT_URL = os.environ.get("YC_ENDPOINT_URL") if os.environ.get("YC_ENDPOINT_URL") else ''

        self.s3_session = boto3.session.Session(
            aws_access_key_id=self.YC_ACCESS_KEY_ID,
            aws_secret_access_key=self.YC_SECRET_ACCESS_KEY,
            region_name=self.YC_REGION_NAME)

        self.s3 = self.s3_session.client(
            service_name='s3',
            endpoint_url=self.YC_ENDPOINT_URL
        )
        # Основные настройки
        self.md = md
        self.srcfiles = srcfiles
        self.actions = actions
        self.metadata = metadata

        self.metadata_source = 's3' if self.metadata[0:3] == 's3a' else 'local'
        if self.metadata_source == 'local':
            with open(metadata, "r") as f1:
                self.md_params = yaml.safe_load(f1)
            with open(self.icebergtbl_props, "r") as f2:
                self.tbl_props_params = yaml.safe_load(f2)

        elif self.metadata_source == 's3':
            # пропускаем s3a://
            metadata = metadata.split(self.bucket_default, 1)[1][1:]
            try:
                res = self.s3.get_object(Bucket=self.bucket_default, Key=metadata)
            except Exception as e:
                self.__print(f'{e} bucket={self.bucket_default} key={metadata}')
                quit()

            self.md_params = yaml.safe_load(res['Body'].read().decode('utf-8'))
            # bucket + prefix
            self.srcbucket = self.md_params[md].get('srcbucket', self.bucket_default)
            res = self.s3.get_object(Bucket=self.bucket_default, Key=f'0datasource/icebergtbl_props.yaml')
            self.tbl_props_params = yaml.safe_load(res['Body'].read().decode('utf-8'))

        # Определяем имя лог-файла со списком обработанных файлов
        if metadata.rfind('/') != -1:
            self.srcfiles_log = metadata[:metadata.rfind('/') + 1] + self.srcfile_log_name
        else:
            self.srcfiles_log = self.srcfile_log_name

        # s3path = f'{self.metadata.rpartition('/')[0]}/{logfile}'
        # pattern = r"s3a://([^/]+)/(.+)"
        # match = re.match(pattern, s3path)
        # if match:
        #     bucket, self.logfile_s3key = match.group(1), match.group(2)

        self.prev_ts = datetime.datetime.now()
        self.start_ts = self.prev_ts

        self.tbl_name = self.md_params[md]['table_name']
        self.srctbl_name = self.md_params[md]['src_table_name']
        attr_queries_str = f'[{self.md_params[md].get("attr_queries", "")}]'
        try:
            self.ds_list = json.loads(attr_queries_str)
        except Exception as e:
            print('{0} {1}'.format(self.__get_time(), e))
            self.ds_list = []

        exp_d = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(days=-1)
        self.exp_ts = exp_d.strftime("%Y-%m-%d %H:%M:%S.000")
        self.database = self.md_params[md].get('database', self.database_default)
        self.sparkdb = "{0}.{1}".format(spark_const.spark_catalog, self.database)
        self.srcformat = self.md_params[md]['srcformat']
        self.loadmanytimes = self.md_params[md].get('loadmanytimes', '')
        self.__init_params()

    def get_s3(self, n: str) -> List[str]:
        return self.__get_srcfiles_list_from_s3(n)

    def __get_srcfiles_list_from_s3(self, n: str) -> List[str]:
        """Возвращает список файлов с данными из заданной папки в заданном бакете s3
        файлы SUCCESS исключаются
            Пример:
             cmlc07p327/000000_0
             cmlc07p327/000001_0
             преобразует в 
             ['000000_0', '000001_0']
        Returns:
            List[str]: _список файлов
        """
        prefix = f'{self.md}/t{n}' if n != '0' else self.md
        res = self.s3.list_objects_v2(Bucket=self.srcbucket, Prefix=prefix, MaxKeys=90000)
        resource_list = list()
        # print(json.dumps(res, indent=2, default=str))
        for item in res['Contents']:
            resource_list.append(str(item['Key']).partition('/')[2])
        resource_list = [r for r in resource_list if '_SUCCESS' not in r]
        return resource_list

    def __prepare_srcfiles_list(self, n: str = '0') -> List[str]:
        """Получение списка файлов для обработки. Итоговый список o_srcfiles
        формируется следующим образом:
        Если на вход поступает непустой список файлов, то он берется за основу.
        Если на вход поступеет пустой список файлов, то метод ищет список в config.yaml и берет
        за основу его.

        Далее из сформированного основного списка исключаются все файлы, которых нет в логе
        загрузки. Это необходимо для исключения (или контроля) повторной загрузки одних и тех же данных

        Returns:
            List[str]: список имен файлов данных для загрузки
        """
        processed_srcfiles = []
        suffix = n if n != '0' else ''
        logfile = f'{self.srcfiles_log}{suffix}.log'
        if self.metadata_source == 'local':
            try:
                with open(logfile, 'r') as f:
                    lines = f.readlines()
                processed_srcfiles = [line.strip() for line in lines]
            except Exception as e:
                self.__print(f'{e}')
                processed_srcfiles = []
        elif self.metadata_source == 's3':
            try:
                res = self.s3.get_object(Bucket=self.srcbucket, Key=logfile)
                content2 = res['Body'].read().decode('utf-8')
                processed_srcfiles = str(content2).split('\n')
            except Exception as e:
                self.__print(f'{e} bucket={self.srcbucket} key={logfile}')            

        if self.srcfiles == []:
            try:
                o_srcfiles = [x['f'] for x in json.loads(f'[{self.md_params[self.md]['srcfiles']}]')]
                print(o_srcfiles)
            except Exception as e:
                print('{0} Ошибка чтения значения для {1}'.format(self.__get_time(), e))
                o_srcfiles = list()
# Если в config.yaml нет файлов И указано имя бакета, то грузим список файлов из бакета
            finally:
                if (not o_srcfiles) and (self.srcbucket):
                    o_srcfiles = self.__get_srcfiles_list_from_s3(n)
        else:
            o_srcfiles = self.srcfiles
        o_srcfiles = [item for item in o_srcfiles if item not in processed_srcfiles]
        return o_srcfiles
        
    def __srcfile_processed(self, action: str, srcfile: str):
        """Действия с журналом загрузки
        Если loadmanytimes = True, то в запись в журнал не выполняется, что 
        позволяет загружать файл несколько раз.

        Args:
            action(Str): тип действия: aX - append file, pX - clear log
            X - обязательный номер таблицы (0,1,2 или 5)
            srcfile (str): имя файла для сохранения в журнале файлов загрузки 
        """
        suffix = action[1] if action[1] != '0' else ''
        logfile = f'{self.srcfiles_log}{suffix}.log'
        if self.metadata_source == 'local':
            if (action[0] == 'a') and (not self.loadmanytimes):
                try:
                    with open(logfile, 'a') as f:
                        f.write(srcfile + '\n')
                    self.__print(f'Файл {srcfile} записан в лог')
                except Exception as e:
                    self.__print(f'Action {action}. Ошибка {srcfile} : {e}')
            elif action[0] == 'p':
                try:
                    with open(logfile, 'w') as f:
                        f.close()
                    self.__print(f'Лог {logfile} очищен')
                except Exception as e:
                    self.__print(f'Action {action}. Ошибка {srcfile} : {e}')
        elif self.metadata_source == 's3':
            if (action[0] == 'a') and (not self.loadmanytimes):
                try:
                    processed_srcfiles = []
                    res = self.s3.get_object(Bucket=self.srcbucket, Key=logfile)
                    content2 = res['Body'].read().decode('utf-8')
                    processed_srcfiles = str(content2).split('\n')
                except Exception as e:
                    self.__print(f'{e} Bucket {self.srcbucket}, Key {logfile}')
                finally:
                    processed_srcfiles.append(srcfile)
                    s3body = '\n'.join(processed_srcfiles)
                    try:
                        self.s3.put_object(Bucket=self.srcbucket, Key=logfile, Body=s3body)
                    except Exception as e:
                        self.__print(f'{e} Bucket {self.srcbucket}, Key {logfile}')            
            elif action[0] == 'p':
                forDeletion = [{'Key': logfile}]
                try:
                    res = self.s3.delete_objects(Bucket=self.srcbucket, Delete={'Objects': forDeletion})
                    self.__print(f'Лог {logfile} очищен')
                except Exception as e:
                    self.__print(f'{e}, Delete {forDeletion} from bucket {self.srcbucket}')

    def __init_params(self):
        """Считывание метаданных загрузки из config.yaml в атрибуты класса
        """
        self.dstype = self.md_params[self.md].get('dstype', 'iobj')
        tbl_props_grp = self.md_params[self.md]['iceberg_tbl_props']
        self.tbl_props = self.tbl_props_params[tbl_props_grp]
        self.key_fields = self.md_params[self.md].get('key_fields', '')
        self.key_fields_create = self.md_params[self.md].get('key_fields_create', '')
        self.sum_keyfigures = self.md_params[self.md].get('sum_keyfigures', '1 as rowcnt')
        self.create_table = self.md_params[self.md].get('create_table', list())

        partition = self.md_params[self.md].get('partition', '')
        self.partition = f'PARTITIONED BY ( {partition} )' if partition != '' else ''

        self.insert_table = self.md_params[self.md].get('insert_table', list())
        self.insert_values = self.md_params[self.md].get('insert_values', list())
        self.merge = self.md_params[self.md].get('merge', list())
        self.merge15 = self.md_params[self.md].get('merge15', list())
        self.insert_where = self.md_params[self.md].get('insert_where', '(1=1)')
        self.insert_order = self.md_params[self.md].get('insert_order', '')
        self.views = self.md_params[self.md].get('views', '')
        self.safe_dml = TypeAdapter(bool).validate_python(self.md_params[self.md].get('safe_dml', self.safe_dml_default))
        self.add_identifier = self.md_params[self.md].get('add_identifier', '')

    def __print(self, msg: str):
        """Метод-заглушка для сохранения журнала обработки. 
            Пока не реализовано иное, просто выполняется печать на экран
        Args:
            msg (str): _description_
        """
        print(msg)

    def __print_init_params(self):
        """вывод в лог существенных параметров запуска сессии iceload
        """
        self.__print("{0} INITIALIZATION PARAMETERS".format(self.__get_time()))
        self.__print("   actions={0}".format(self.actions))
        self.__print("   metadata={0}".format(self.metadata))        
        self.__print("   srcfiles log={0}".format(self.srcfiles_log))        
        self.__print("   tablename={0}".format(self.tbl_name))
        self.__print("   dstype={0}".format(self.dstype))
        self.__print("   srctablename={0}".format(self.srctbl_name))
        self.__print("   expiration ts={0}".format(self.exp_ts))
        self.__print("   src format={0}".format(self.srcformat))
        self.__print("   src bucket={0}".format(self.srcbucket))
        self.__print("   safe dml={0}".format(self.safe_dml))
        self.__print("   source files={0}".format(self.srcfiles))
        self.__print("   spark database={0}".format(self.sparkdb))

    def __get_time(self) -> str:
        """Генерация строки с текущим временем и длительностью с момента предыдущего вызова.
        Нужно для отображения в логах, для визуального контроля длительности выполнения 

        Returns:
            str: строка с текущим временем и длительностью (в сек) с предыдущего вызова метода
        """
        ts = datetime.datetime.now()
        distance = (ts - self.prev_ts)
        output = f'{ts.strftime("%Y-%m-%d %H:%M:%S")} {distance.seconds:04}s.'
        self.prev_ts = ts
        return output

    def __get_timestr(self) -> str:
        """Генерация строки с текущим временем .
        Нужно для добавления в имя 

        Returns:
            str: строка с текущим временем
        """
        ts = datetime.datetime.now()
        return f'{ts.strftime("%Y%m%d%H%M%S")}'

    def __get_total_time(self) -> str:
        """метод для вывода длительности работы сессии iceload

        Returns:
            str: строка с длительностью работы сессии iceload
        """
        distance = (datetime.datetime.now() - self.start_ts)
        return f'{distance.seconds:05}s.'

    def __init_spark(self):
        """Создание экземпляра spark-приложения для сессии iceload
        """
        self.spark = SparkSession.builder.master("local[2]").config(conf=spark_const.conf_2g_ice_warehouse2).getOrCreate()
        self.spark.sql("SET spark.sql.ansi.enabled=true").show(1)
        self.__print('{0} SPARK getOrCreate'.format(self.__get_time()))

    def __stop_spark(self):
        """Остановка экземпляра spark-приложения для сессии iceload
        """
        self.spark.stop()
        self.__print('{0} SPARK stopped'.format(self.__get_time()))

    def finish(self):
        """Остановка экземпляра spark-приложения и завершение работы сессии
        """
        self.__stop_spark()
        self.__print("{0} That's it. Total is {1}".format(self.__get_time(), self.__get_total_time()))

    def run_action(self):
        """Последовательный запуск (в цикле) определенных для сессии действий.
        Также, выполняется установка каталог.бд по умолчанию для всех SQL-команд
        """
        self.__print_init_params()
        self.__init_spark()
        self.spark.sql("use {0}".format(self.sparkdb)).show(10)
        for item in self.actions:
            if (item == 'drop') or (item == 'drop0'):
                self.__action_drop()
            elif item == 'drop1':
                self.__action_drop('1')
            elif item == 'drop2':
                self.__action_drop('2')
            elif (item == 'delete') or (item == 'delete0'):
                self.__action_delete()
            elif item == 'delete1':
                self.__action_delete('1')
            elif item == 'delete2':
                self.__action_delete('2')
            elif item == 'delete5':
                self.__action_delete('5')
            elif (item == 'create') or (item == 'create0'):
                self.__action_create()
            elif item == 'create1':
                self.__action_create('1')
            elif item == 'create2':
                self.__action_create('2')
            elif (item == 'dropcreate') or (item == 'dropcreate0'):
                self.__action_dropcreate()
            elif item == 'dropcreate1':
                self.__action_dropcreate('1')
            elif item == 'dropcreate2':
                self.__action_dropcreate('2')
            elif item == 'dropcreate5':
                self.__action_dropcreate('5')
            elif (item == 'insert') or (item == 'insert0'):
                self.__action_insert()
            elif item == 'insert1':
                self.__action_insert('1')
            elif item == 'insert2':
                self.__action_insert('2')
            elif item == 'insert5':
                self.__action_insert('5')
            elif item == 'recreate_attr':
                self.__action_recreate_attr()
            elif item == 'merge12':
                self.__action_merge('12')
            elif item == 'views':
                self.__action_views()
            elif item == 'checks':
                self.__action_checks()
            else:
                self.__print('{0} - неизвестная команда'.format(item))

    def __action_drop(self, n: str = '0'):
        """Действие: удаление таблиц из каталога и ее файлов.
        Также, очищается лог имен файлов с данными, которые ранее были загружены в эту таблицу

        Args:
            n (str, optional): номер таблицы, соотв. таблице в ADSO (1,2 или 5). 
            Добавляется к имени таблице в SQL-операциях Если 0, то ничего не добавляется.
            Defaults to '0'.
        """
        if n == '0':
            tname = self.tbl_name
        if n in ['1', '2', '5']:
            tname = '{0}{1}'.format(self.tbl_name, n)
        query = '''DROP TABLE IF EXISTS {1}.{0} PURGE'''.format(tname, self.sparkdb)
        self.spark.sql(query).show(1, truncate=False)
        self.__srcfile_processed(f'p{n}', '')
        self.__print('{0} DROP TABLE {2}.{1}'.format(self.__get_time(), tname, self.sparkdb))

    def __action_create(self, n: str = '0'):
        """Действие: создание или пересоздание таблицы
        Также, очищается лог имен файлов с данными, которые ранее были загружены в эту таблицу
        
        В зависимости от настройки в метаданных iceload для таблицы, выполняется создание
        идентификаторов (== первичных ключей) в таблице. Это нужно только для Flink и
        будущих сценариев. Для транзакционных данных отключено, т.к. из-за бага при наличии
        идентификаторов иногда не работает MERGE

        Args:
            n (str, optional): номер таблицы, соотв. таблице в ADSO (1,2 или 5). Defaults to '0'.
        """
        if n in ['0']:
            tname = self.tbl_name
            tcreate_table = self.create_table
        if n == '1':
            tname = '{0}{1}'.format(self.tbl_name, n)
            tcreate_table = '{0}, {1}'.format(self.request_fields_create, self.create_table)
        if n in ['2', '5']:
            tname = '{0}{1}'.format(self.tbl_name, n)
            tcreate_table = self.create_table
        query = '''CREATE OR REPLACE TABLE {1}.{0} ({3}) USING iceberg {4} TBLPROPERTIES {2}
                '''.format(tname, self.sparkdb, self.tbl_props, tcreate_table, self.partition)
        # print(query)
        self.spark.sql(query).show(1, truncate=False)
        if self.add_identifier and n == '0':
            query = '''ALTER TABLE {1}.{0} SET IDENTIFIER FIELDS {2}'''\
                .format(tname, self.sparkdb, self.key_fields)
            self.spark.sql(query).show(1, truncate=False)
        self.__srcfile_processed(f'p{n}', '')
        self.__print('{0} CREATE TABLE {2}.{1}'.format(self.__get_time(), tname, self.sparkdb))      

    def __action_dropcreate(self, n: str = '0'):
        """Действие: объединение двух действий в одно: удаление и создание таблицы

        Args:
            n (str, optional): номер таблицы, соотв. таблице в ADSO (1,2 или 5). Defaults to '0'.
        """
        self.__action_drop(n)
        self.__action_create(n)

    def __action_delete(self, n: str = '0'):
        """Удаление данных (командой DELETE) из заданной таблицы n
        необходимо выполнять удаление из таблицы 1 данных, которые были успешно
        смерджены в таблицу 2 или 5 (для некумулятивных ADSO)
        Args:
            n (str, optional): номер таблицы. Defaults to '0'.
        """
        tname = ''
        if n in ['0']:
            tname = self.tbl_name
        elif n == '1':
            tname = '{0}{1}'.format(self.tbl_name, n)
        elif n in ['2', '5']:
            tname = '{0}{1}'.format(self.tbl_name, n)
        if tname:
            query = 'DELETE FROM {0}.{1}'.format(self.sparkdb, tname)
            self.spark.sql(query).show(10, truncate=False)
            self.__print('{0} DELETE FROM {2}.{1}'.format(self.__get_time(), tname, self.sparkdb))     
            self.__post_processing(tname)

    def __action_insert(self, n: str = '0'):
        """Действие: вставка записей в таблицу. Данные для вставки - из srcfiles.
        поддерживаются форматы csv, orc, parquet и s3-orc. Первые три формата - файлы хранятся
        локально, в файловой системе spark. Последующие форматы, с префиксом s3- - в objectStorage
        После создания spark-dataframe на основе файла данных, создается tempview, 
        который является объектом SQL, а значит - может использоваться для вставки в SELECT-запросе
        После успешной вставки файл данных добавляется в лог обработанных файлов для контроля
        повторной обработки

        Args:
            n (str, optional): номер таблицы, соотв. таблице в ADSO (1,2 или 5).
            0 - обычно для таблиц справочников, которые в SAP BW не имеют цифрового суффикса
            Defaults to '0'.
        """
        if n in ['0']:
            tname = self.tbl_name
            tinsert_table = self.insert_table
            tinsert_values = self.insert_values
        elif n == '1':
            tname = '{0}{1}'.format(self.tbl_name, n)
            tinsert_table = '{0}, {1}'.format(self.request_fields, self.insert_table)
            tinsert_values = f'reqtsn, datapakid, cast(record as INT), {self.insert_values}'       
        elif n in ['2', '5']:
            tname = '{0}{1}'.format(self.tbl_name, n)
            tinsert_table = self.insert_table
            tinsert_values = self.insert_values
        self.srcfiles = self.__prepare_srcfiles_list(n)
        for li in self.srcfiles:
            if self.srcformat == 'csv':
                delimiter = self.md_params[self.md].get('delimiter', ',')
                df = self.spark.read.options(delimiter=delimiter, header=True, escape="\\").csv(li)
            elif self.srcformat == 'orc':
                df = self.spark.read.orc(li)
            elif self.srcformat == 'parquet':
                df = self.spark.read.parquet(li)
            elif self.srcformat == 's3-orc':
                df = self.spark.read.orc(f's3a://{self.srcbucket}/{self.md}/{li}')
            elif self.srcformat == 's3-parquet':
                df = self.spark.read.parquet(f's3a://{self.srcbucket}/{self.md}/{li}')
            
            suffix = n if n != '0' else ''
            tsrctbl_name = f'{self.srctbl_name}{suffix}'
            df.createOrReplaceTempView(tsrctbl_name)

            if not self.safe_dml:  # вставка в основную ветку main
                query = '''INSERT INTO {1}.{0} ({2}) (
                            SELECT {3} FROM {4} WHERE {5} {6})'''\
                        .format(tname, self.sparkdb, tinsert_table,
                                tinsert_values, tsrctbl_name, self.insert_where,
                                '' if self.insert_order == '' else ' ORDER BY {0}'.format(self.insert_order))
                self.spark.sql(query).show(10, truncate=False)
            else:  # вставка в отдельную ветку и merge c основной веткой в случае успеха. И ничего в случае неуспеха проверки
                branch = f'new{self.__get_timestr()}'
                query = """ALTER TABLE {0}.{1} CREATE BRANCH `{2}` RETAIN 2 DAYS""".format(self.sparkdb, tname, branch)
                self.spark.sql(query).show(30, truncate=False)
                query = '''INSERT INTO {1}.{0}.branch_{7} ({2}) (
                            SELECT {3} FROM {4} WHERE {5} {6})'''\
                        .format(tname, self.sparkdb, tinsert_table,
                                tinsert_values, tsrctbl_name, self.insert_where,
                                '' if self.insert_order == '' else ' ORDER BY {0}'.format(self.insert_order), branch)
                self.spark.sql(query).show(10, truncate=False)
                checks = self.__action_checks()
                self.__print('{0} Checks with {2}.{1}={3}'.format(self.__get_time(), tname, self.sparkdb, checks))                 
                if checks:
                    query = "CALL system.fast_forward('{0}.{1}', 'main', '{2}')".format(self.sparkdb, tname, branch)
                    self.__print('{0} Fast_forward {2}.{1}: main->{3}'.format(self.__get_time(), tname, self.sparkdb, branch))  
                else:
                    query = "ALTER TABLE {0}.{1} DROP BRANCH `{2}`".format(self.sparkdb, tname, branch)
                    self.__print('{0} Drop branch {3} from {2}.{1}'.format(self.__get_time(), tname, self.sparkdb, branch))  
                self.spark.sql(query).show(30, truncate=False)

            self.__srcfile_processed(f'a{n}', li)
            self.__print('{0} INSERT INTO {2}.{1}'.format(self.__get_time(), tname, self.sparkdb))     
            self.__post_processing(tname)

    def __action_recreate_attr(self):
        """Действие: пересоздания атрибутов.
        Специфическое действие для основных данных, в котором из одного исходного файла
        с денормализованными данными по признаку создаются и заполняются несколько таблиц, соответствующиз
        атрибутам основных данных.
        Состав таблиц и запросы для их заполнения описаны в метаданных таблицы для iceload (cм. attr_queries в config.yaml)
        """
        for li in self.ds_list:
            try:
                query = '''CREATE OR REPLACE TABLE {1}.dwh_t_{0}
                        ( {0} STRING NOT NULL, {0}_txt STRING)
                        USING iceberg TBLPROPERTIES {2}'''.format(li['t'], self.sparkdb, self.tbl_props)
                self.spark.sql(query).show(10)
                query = '''ALTER TABLE {1}.dwh_t_{0} SET IDENTIFIER FIELDS {0}'''.format(li['t'], self.sparkdb)
                self.spark.sql(query).show(10)

                query = '''INSERT INTO {2}.dwh_t_{0} ({1})'''.format(li['t'], li['q'], self.sparkdb)
                self.spark.sql(query).show(10)
                
                self.__post_processing("dwh_t_" + li['t'])
            except Exception as e:
                self.__print('{0} {1}'.format(self.__get_time(), e))
            finally:
                self.spark.sql(f"SELECT * FROM {self.sparkdb}.dwh_t_{li['t']}").show(20, truncate=False)

    def __action_merge(self, n: str = '12'):
        """Действие: выполнение команды MERGE.
        В зависимости от dstype конструируются и выполняются различные MERGE-запросы, которые
        копируют данные из таблицы 1 в таблицу 2 (в случае dstype=adso-nc - еще и в таблицу 5 (для "маркера" остатков))
        После успешного выполнения merge выпоняется удаление записей в 1-таблице
        для dstype=adso-nc метод находится в статусе "доработка"

        Args:
            n (str, optional): номера таблиц (откуда->куда), соотв. таблице в ADSO (1,2).
            Defaults to '0'.
            Если adso-ncum, то внутри merge 12 добавляется merge 15
        """
        target_table = '{0}{1}'.format(self.tbl_name, n[1])
        source_table = '{0}{1}'.format(self.tbl_name, n[0])
        if (self.dstype == 'adso-std') and (n == '12'):
            query = """ CREATE OR REPLACE TABLE {0}.dwh_k_{1}1 ({2}, {3} )
                    USING iceberg TBLPROPERTIES {4}""".format(self.sparkdb, self.md, self.request_fields_create,
                                                              self.key_fields_create, self.tbl_props)
            self.spark.sql(query).show(1)
            query = """INSERT INTO {0}.dwh_k_{1}1 (
                        {2},{4} {3},{4} FROM {0}.dwh_t_{1}1
                        GROUP BY {4}
                        ORDER BY {4}))""".format(self.sparkdb, self.md, self.k_table_prefix1, self.k_table_prefix2, self.key_fields)
            # print(query)
            self.spark.sql(query).show(1)
            self.__print("CREATE AND FILL TABLE {0}.dwh_k_{1}1".format(self.sparkdb, self.md))
            self.__post_processing('dwh_k_{0}1'.format(self.md))
            query = '''MERGE INTO {1}.{0} AS target
                    USING (SELECT t1.* FROM {1}.{2} as t1, {1}.dwh_k_{4}1 as t2
                            WHERE {5}) AS source {3}'''\
                .format(target_table, self.sparkdb, source_table,
                        self.merge, self.md, self.request_fields_join)
            self.spark.sql(query).show(2)
            self.__print('{0} MERGE{3} INTO {2}.{1}'.format(self.__get_time(), target_table, self.sparkdb, n))
            self.__action_delete("1")  # как и при активации ADSO - удаляем уже обработанные данные
            self.__post_processing(target_table)
        elif (self.dstype in ['adso-cube', 'adso-ncum']) and (n == '12'):
            query = '''MERGE INTO {1}.{0} AS target USING (SELECT
                        {2}, {3} FROM {1}.{4} GROUP BY {2}) AS source ON {5}'''\
                .format(target_table, self.sparkdb, self.key_fields, self.sum_keyfigures,
                        source_table, self.merge)
            print(query)
            self.spark.sql(query).show(2)
            self.__print('{0} MERGE {3} INTO {2}.{1}'.format(self.__get_time(), target_table, self.sparkdb, n))
            self.__post_processing(target_table)
            if self.dstype == 'adso-ncum':
                target_table5 = '{0}{1}'.format(self.tbl_name, '5')
                key_fields5 = self.key_fields.upper().replace("CALDAY,", "")
                query = '''MERGE INTO {1}.{0} AS target USING (SELECT MIN(calday) as CALDAY, 
                            {2}, {3} FROM {1}.{4} GROUP BY {2}) AS source ON {5}'''\
                    .format(target_table5, self.sparkdb, key_fields5, self.sum_keyfigures,
                            source_table, self.merge15)
                print(query)
                self.spark.sql(query).show(2)
                self.__print('{0} MERGE {3} INTO {2}.{1}'.format(self.__get_time(), target_table5, self.sparkdb, n))
                self.__post_processing(target_table5)
            self.__action_delete("1")  # как и при активации ADSO - удаляем
        else:
            pass

        self.__print('{0} MERGE{3} INTO {2}.{1}'.format(self.__get_time(), target_table, self.sparkdb, n))
        self.__post_processing(target_table)

    def __post_processing(self, tname):
        """Постобработка таблицы iceberg после вставки/merge, необходимая для оптимизации состава файлов
        данных таблицы и производительности операций с ней

        Args:
            tname (_type_): имя таблицы (без указания каталога и БД)
        """
        self.spark.sql('''CALL system.rewrite_manifests( table => '{1}.{0}'
                    ,use_caching => True)'''.format(tname, self.sparkdb))
        self.__print('{0} REWRITE MANIFESTS from {2}.{1}'.format(self.__get_time(), tname, self.sparkdb))
        self.spark.sql('''CALL system.expire_snapshots(table => '{2}.{0}',
                older_than => TIMESTAMP '{1}',
                retain_last => 1)'''.format(tname, self.exp_ts, self.sparkdb))
        self.__print('{0} EXPIRE SNAPSHOTS from {2}.{1}'.format(self.__get_time(), tname, self.sparkdb))
        self.spark.sql('''CALL system.remove_orphan_files(table => '{2}.{0}',
                older_than => TIMESTAMP '{1}',
                dry_run => False)'''.format(tname, self.exp_ts, self.sparkdb))
        self.__print('{0} REMOVE ORPHAN FILES from {2}.{1}'.format(self.__get_time(), tname, self.sparkdb))
        self.spark.sql('''CALL system.rewrite_position_delete_files(table => '{1}.{0}')'''.format(tname, self.sparkdb))
        print('{0} REMOVE REWRITE POS DELETES from {2}.{1}'.format(self.__get_time(), tname, self.sparkdb))

    def __action_views(self):
        """Действие: создание ракурсов данных. Перечень ракурсов закодирован в параметре views в config.yaml
        12 - union для 2-х таблиц 1 и 2
        Метод находится в статусе "доработка". 
        """
        views_lst = str(self.views).split(',')
        for v in views_lst:
            query = ''
            if v == '12':
                query = "CREATE VIEW IF NOT EXISTS {0}.dwh_v_{1}7 AS \
                    (SELECT {2} FROM {0}.dwh_t_{1}1 \
                    UNION ALL SELECT {2} FROM {0}.dwh_t_{1}2)".format(self.sparkdb, self.md,
                    self.insert_table)
            if v == '125':
                query = ''
            if query:
                # self.spark.sql(query).show(10)
                print(query)
                self.__print("{0} VIEWS in {2} FOR {1}".format(self.__get_time(), self.md, self.sparkdb))
 
    def __action_checks(self) -> bool:
        """Действие: запуск проверок данных на консистентность и сходимость.
        Запросы для проверок определены в config.yaml
        """
        checks = True
        self.__print("{0} Проверки для {1} выполнены. См. журнал".
                     format(self.__get_time(), self.md))
        if checks:
            return True
        else:
            return False
