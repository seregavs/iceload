# Как загружать ADSO?
## adso-cube
### Инициализация
1. BW: заблокировать операции активации и загрузки ADSO в BW
2. CDP: выгрузить 1- и 2-таблицы в orc на YC, в папки stg-bi-1/<adsoname>/t1|t2
3. BW|CDP: запомнить (в источнике: в BW или CDP) номер максимального реквеста в 1-таблице, загрузку ADSO можно продолжать
4. BW: разблокировать операции активации и загрузки ADSO в BW
5. NOVA: загрузить orc соотв. 1 и 2 iceberg-таблицы командами insert1, insert2. (в config.yaml: srcfiles - пуст, srcbucket = stg-bi-1, srcformat = s3-orc,   loadmanytimes = False)

### Дельта
1. CDP: Считать номер максимального реквеста
2. CDP: выгрузить 1-таблицу, все реквесты, большие номера максимального, в orc на YC, в папку stg-bi-1/<adsoname>/t1
3. BW|CDP: запомнить (в источнике: в BW или CDP) номер максимального выгруженного реквеста из 1-таблицы, загрузку ADSO можно продолжать
4. NOVA: загрузить orc в 1 iceberg-таблицу командой insert1 (в config.yaml: srcfiles - пуст, srcbucket = stg-bi-1, srcformat = s3-orc, loadmanytimes = False)
5. NOVA: (опционально). Выполнить команду merge12 (сожмет все реквесты 1->2, очистит таблицу 1)

## adso-std
### Инициализация
1. BW: активировать все реквесты в ADSO (чтобы 1-таблица была пустой)
2. BW: заблокировать операции активации и загрузки ADSO в BW
3. CDP: выгрузить 2-таблицу в orc на YC, в папку stg-bi-1/<adsoname>/t2
4. BW|CDP: запомнить (в источнике: в BW или CDP) номер максимального реквеста в 3-таблице
5. BW: разблокировать операции активации и загрузки ADSO в BW
6. NOVA: загрузить orc 2-iceberg-таблицу командой insert2 (в config.yaml: srcfiles - пуст, srcbucket = stg-bi-1, srcformat = s3-orc, loadmanytimes = False)
### Дельта
1. CDP: Считать номер максимального реквеста
2. CDP: выгрузить 3-таблицу, все реквесты, большие номера максимального, в orc на YC, в папку stg-bi-1/<adsoname>/t1 (выгружать записи с RECORDMODE in ('N','','D'))
3. BW|CDP: запомнить (в источнике: в BW или CDP) номер максимального выгруженного реквеста из 3-таблицы
4. NOVA: загрузить orc в 1 iceberg-таблицу командой insert1 (в config.yaml: srcfiles - пуст, srcbucket = stg-bi-1, srcformat = s3-orc, loadmanytimes = False)
5. NOVA: Выполнить команду merge12 (сожмет все реквесты 1->2, очистит таблицу 1)