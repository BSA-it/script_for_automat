"""
Программа создает файлы-исходники в папке it\Иван\ИВАН\НовыйАвтомат(НК/ВБ/МАЙ)\Исходники(НК/ВБ/МАЙ)CRM , необходимые для автоматов Ивана.
Логика основанана на сверке данных из JSON застройщика, где содержатся свободные квартиры, и "Эталонных выгрузок", в которых содержатся данные по всем квартирам вообще
При выводе новой секции/корпуса необходимо внести данные в эталонные выгрузки.
Для котроля работы можно раскомментировать закомментированные строки кода, которые создают промежуточные .csv таблицы в директории проекта
Точка роста - ликивдация необходимости ссылаться на Эталонные выгрузки, как вариант ссылаться на автомат Ивана.

"""

import requests
import json
import pandas as pd

def get_json():
    url = 'http://incrm.ru/export-tred/ExportToSite.svc/ExportToTf/json' #адрес JSON выгрузки из CRM застройщика
    r = requests.get(url) #заправшиваем адрес
    json_data=json.loads(r.text) #получаем выгрузку в виде текста
    data_frame = pd.DataFrame.from_records(json_data,columns = ["ArticleID", "Article", "Number", "StatusCode", "StatusCodeName", "Quantity", "Rooms", "Sum",
                       "Finishing", "Decoration", "SeparateEntrance","RoofExit","2level","TerrasesCount"]) #загружаем выгрузку в DataFrame
    return data_frame #возвращаем выгрузку в виде DataFrame

def maintain_df(data_frame,zhk):
    data_frame = data_frame.rename(
        columns={'Article': 'Код объекта', 'Number': 'Номер квартиры', 'StatusCodeName': 'Статус',
                 'Quantity': 'Площадь',
                 'Sum': 'Цена', 'Decoration': 'Отделка'}) #переименовываем в DataFrame названия столбцов на русские
    data_frame = data_frame.drop(
        columns=['ArticleID', 'StatusCode', 'Finishing', 'SeparateEntrance', 'RoofExit', '2level',
                 'TerrasesCount']) #Выкидываем ненужные столбцы
    data_frame['Rooms'].replace({'0': 'СТ', '1': '1К', '2': '2К', '3': '3К', '4': '4К'}, inplace=True)
    data_frame['Цена'] = data_frame['Цена'].astype(float)
    data_frame['Площадь'] = data_frame['Площадь'].astype(float) # Приводим данные в формат десятичных дробей
    data_frame['Цена за метр'] = data_frame['Цена'] / data_frame['Площадь'] # Высчитываем цену за метр
    data_frame = data_frame.replace(
        {'без отделки': 0, 'б/о с перегородками': 0, 'НЕКОНДИЦИЯ': 0, 'Классика': 2, 'МОДЕРН': 2, 'СОЧИ': 2,
         'Финишная отделка': 2, 'ч/о без перегородок': 1, 'черновая': 1, 'чистовая': 2, 'чистовая (светлая)': 2,
         'чистовая (темная)': 2, 'ЯЛТА': 2, 'Без отделки':0, 'Модерн':2, 'Сочи':2, 'Ялта':2, 'Чистовая':2, 'Черновая':1,
         'без отделки (old)':0, 'Венеция':2,'венеция':2,'ВЕНЕЦИЯ':2})

    data_frame = data_frame.assign(service=data_frame['Код объекта'].str.get(0))
    data_frame = data_frame[data_frame['service'] == zhk]
    #data_frame.to_csv(zhk + 'МАЙ.csv', sep=';', encoding='cp1251', decimal=',', float_format='%.2f', index=False)
    data_frame = data_frame[data_frame['Код объекта'].apply(lambda x : 'F' not in x)]
    data_frame.to_csv('DF' + '.csv', sep=';', encoding='cp1251', decimal=',', float_format='%.2f', index=False)
    for i in range(len(data_frame)):
        if (data_frame.iloc[i, data_frame.columns.get_loc('service')] == 'Н'):
           try:
               data_frame.iloc[i, data_frame.columns.get_loc('Код объекта')] = 'Новокрасково, корп. %d, кв.%d' % (int(data_frame.iloc[i, 0][3:4]), int(data_frame.iloc[i, 0][14:]))
           except:
               pass
        elif (data_frame.iloc[i, data_frame.columns.get_loc('service')] == 'В'):
            if (data_frame.iloc[i, 0][:6] == 'ВБ-5,1' or data_frame.iloc[i, 0][:6] == 'ВБ-5,2'):
                data_frame.iloc[i, 0] = 'Видный берег, корп. 5, кв.%d' % (int(data_frame.iloc[i, 0][16:]))
            else:
                data_frame.iloc[i, 0] = 'Видный берег, корп. %d, кв.%d' % (int(data_frame.iloc[i, 0][4:5]), int(data_frame.iloc[i, 0][15:]))
        elif (data_frame.iloc[i, data_frame.columns.get_loc('service')] == 'М'):
            if (data_frame.iloc[i, 0][4:6] == '01' or data_frame.iloc[i, 0][4:6] == '02' or data_frame.iloc[i, 0][4:6] == '05' or data_frame.iloc[i, 0][
                                                                                                          4:6] == '06' or
                    data_frame.iloc[i, 0][4:6] == '09'):
                data_frame.iloc[i, 0] = 'Май, корп. %d, кв.%d' % (int(data_frame.iloc[i, 0][4:6]), int(data_frame.iloc[i, 0][16:]))
            else:
                data_frame.iloc[i, 0] = 'МАЙ, корп. %d, кв.%d' % (int(data_frame.iloc[i, 0][4:6]), int(data_frame.iloc[i, 0][16:]))
    #data_frame.to_csv(zhk + '.csv', sep=';', encoding='cp1251', decimal=',', float_format='%.2f', index=False)
    return data_frame

def merge(data_frame, file):
    path = 'C:\\Python\\for_automat\\'
    prev_df = pd.read_csv(path+'_'+file+'.csv',sep=';', encoding='cp1251',engine='python', index_col=False)
    prev_df['Номер квартиры'] = prev_df['Номер квартиры'].astype(int)
    prev_df.rename(columns={'Название':'Код объекта'},inplace=True)
    mer_df = pd.merge(prev_df, data_frame, how='left', on='Код объекта')
    #mer_df.to_csv('result-ВВ'+ '.csv', sep=';', encoding='cp1251', decimal=',', float_format='%.2f', index=False)
    for i in range(len(mer_df)-9):
        if(pd.isnull(mer_df.iloc[i,mer_df.columns.get_loc('Цена_y')])):
            mer_df.iloc[i, mer_df.columns.get_loc('Цена_y')]=float(mer_df.iloc[i, mer_df.columns.get_loc('Цена_x')].replace(",", "."))
        if(pd.isnull(mer_df.iloc[i,mer_df.columns.get_loc('Площадь_y')])):
            mer_df.iloc[i, mer_df.columns.get_loc('Площадь_y')]=float(mer_df.iloc[i, mer_df.columns.get_loc('Площадь_x')].replace(",", "."))
        if(pd.isnull(mer_df.iloc[i,mer_df.columns.get_loc('Статус_y')])):
            mer_df.iloc[i, mer_df.columns.get_loc('Статус_y')]='Продано'
        if(pd.isnull(mer_df.iloc[i,mer_df.columns.get_loc('Отделка_y')]) and pd.notnull(mer_df.iloc[i,mer_df.columns.get_loc('Отделка_x')])):
            mer_df.iloc[i, mer_df.columns.get_loc('Отделка_y')]=int(mer_df.iloc[i, mer_df.columns.get_loc('Отделка_x')])
        if (pd.isnull(mer_df.iloc[i, mer_df.columns.get_loc('Rooms')]) and pd.notnull(mer_df.iloc[i, mer_df.columns.get_loc('Комнат')])):
            mer_df.iloc[i, mer_df.columns.get_loc('Rooms')] = mer_df.iloc[i, mer_df.columns.get_loc('Комнат')]
        if(pd.isnull(mer_df.iloc[i,mer_df.columns.get_loc('Цена за метр')])):
            mer_df.iloc[i, mer_df.columns.get_loc('Цена за метр')]=\
                float(mer_df.iloc[i, mer_df.columns.get_loc('Цена_x')].replace(",", "."))/float(mer_df.iloc[i, mer_df.columns.get_loc('Площадь_x')].replace(",", "."))
    mer_df.rename(columns={'Номер квартиры_x':'Номер квартиры','Статус_y':'Статус','Площадь_y':'Площадь','Цена_y':'Цена','Отделка_y':'Отделка'},inplace=True)
    if file=='NK':
        path = '\\\\192.168.10.123\\it\\Иван\\ИВАН\\НовыйАвтоматНК\\ИсходникиНКCRM\\'
    elif file == 'MAY':
        path = '\\\\192.168.10.123\\it\\Иван\\ИВАН\\НовыйАвтоматМАЙ\\ИсходникиМАЙCRM\\'
    elif file == 'VB':
        path = '\\\\192.168.10.123\\it\\Иван\\ИВАН\\НовыйАвтоматВБ\\ИсходникиВБCRM\\'
    mer_df['Rooms'].replace({0: 'CT', 1: '1К', 2: '2К', 3: '3К', 4: '4К'}, inplace=True)
    mer_df.to_csv(path+file + '.csv', sep=';', encoding='cp1251',decimal=',', float_format='%.2f', index=False,columns=['Код объекта','Номер квартиры','Статус','Площадь','Цена','Отделка','Цена за метр','Rooms'])

    return 0

if __name__ == '__main__':
    print('Выбери ЖК, чтобы сделать выгрузку:')
    print('В - Видный берег, М - Май, Н - Новокрасково')
    zhk = input()
    data = get_json()
    if (zhk=='М'):file = 'MAY'
    elif (zhk == 'Н'):file = 'NK'
    elif (zhk == 'В'):file = 'VB'
    data = maintain_df(data,zhk)
    merge(data, file)
    print('Выгружено '+file+'!')