import os
import glob
import pandas as pd
import time
from tqdm import tqdm
import json
import codecs
import sys
from configparser import ConfigParser
import smtplib
from email.message import EmailMessage
from jinja2 import Environment, FileSystemLoader


def save_excel(df, filename):
    # Запись таблицы в файл Excel
    writer = pd.ExcelWriter(filename)
    df.to_excel(writer, 'Лист1', index=True)
    writer.save()


def create_directory():
    path = 'Рассылки'
    directories = [x for x in os.listdir(path) if os.path.isdir(os.path.join(path, x))]
    if directories:
        print('Существующие папки:')
        print(' '.join(directories))
    new_dir = f'Рассылка_{time.strftime("%Y.%m.%d", time.localtime(time.time()))}'
    dir_name = input(f'Новая рассылка: [{new_dir}] ')
    dir_name = dir_name.strip() if dir_name.strip() else new_dir

    if os.path.exists(os.path.join(path, dir_name)):
        r = input('Папка существует. Файлы внутри перезапишутся. Вы уверены? [y/n] ')
        if r == 'y':
            return os.path.join(path, dir_name)
        elif r == 'n':
            print('Тогда повторите')
            return create_directory()
        else:
            print('Что?')
            return create_directory()
    else:
        os.mkdir(os.path.join(path, dir_name))
        return os.path.join(path, dir_name)


def select_directory():
    path = 'Рассылки'
    directories = [x for x in os.listdir(path) if os.path.isdir(os.path.join(path, x))]
    selecter = [f'{str(i)}. {dir}' for i, dir in enumerate(directories)]
    print('Выберете папку для рассылки:')
    print('\n'.join(selecter))
    r = input()
    try:
        dir_name = os.path.join(path, directories[int(r)])
    except ValueError:
        print('Нужно вводить число!')
        dir_name = select_directory()
    except IndexError:
        print('Нет такого номера!')
        dir_name = select_directory()
    return dir_name


class ClientMailing:
    # Класс для формирования списков и рассылки писем

    def __init__(self):
        # Загрузка данных
        print('Загрузка данных...', end='')
        self.path_to_db = 'База'
        self.objects = pd.read_excel('Объекты.xlsx', index_col='#')
        self.tenants = pd.read_excel(os.path.join(self.path_to_db, 'Арендаторы_test.xlsx'), index_col='#')
        self.category = pd.read_excel(os.path.join(self.path_to_db, 'Категории.xlsx'))
        # Загрузка файла конфигурации
        base_path = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_path, "email.ini")
        if os.path.exists(config_path):
            cfg = ConfigParser()
            cfg.read_file(codecs.open(config_path, 'r', encoding='utf-8'))
        else:
            print("Config not found! Exiting!")
            sys.exit(1)
        # extract server and from_addr from config
        # smtp
        self.host = cfg.get("smtp", "server")
        self.port = cfg.get("smtp", "port")
        self.username = cfg.get("smtp", "username")
        self.password = cfg.get("smtp", "password")
        self.from_addr = cfg.get("smtp", "from_addr")
        # user
        self.user = {'FIO': cfg.get("user", "FIO"), 'position': cfg.get("user", "position"),
                     'phone': cfg.get("user", "phone"), 'contact_email': cfg.get("user", "contact_email"),
                     'url_image': cfg.get("user", "url_image")}
        # company
        self.company = {'url_logo': cfg.get("company", "url_logo"),
                        'unsubscribe_email': cfg.get("company", "unsubscribe_email")}
        # Загружаем шаблоны
        self.path_to_templates = os.path.join('scripts_mailing', 'templates')
        print('OK')
        # Очистка данных
        self.objects = self.objects[self.objects['Обработать'] == 'да']
        self.objects['Адрес'] = self.objects['Адрес']
        self.tenants.dropna(subset=['Email'], inplace=True)
        # self.tenants.drop(self.tenants[(self.tenants['Профиль'].isna()) | (self.tenants['Формат'].isna())].index, inplace=True)
        self.tenants.drop(self.tenants[self.tenants['Формат'].isna()].index, inplace=True)

    def create_list_mailing(self):
        # Формирование списков рассылки
        def set_built_type(text, text_dict):
            result = []
            text = text.split(', ')
            for p in text:
                result.append(text_dict[p])
            return min(result)

        def search_tenants(filter_data):
            filter_data = filter_data[(((filter_data['Площадь, от'] <= area) & (filter_data['Площадь, до'] >= area)) |
                                       (filter_data['Площадь, от'].isna())) &
                                      ((filter_data['Цена, до'] >= cost) | (filter_data['Цена, до'].isna())) &
                                      (filter_data['Тип здания'] <= build_type) &
                                      (filter_data['Вентиляция'] <= vent_type) &
                                      (filter_data['Лицензия'] <= lic_type) &
                                      (filter_data['Локация'].str.contains(rf'\b{location}\b', na=True))]
            return filter_data.copy()

        # Добаление категорий
        dict_type_build = {}
        dict_ventilation = {}
        dict_license = {}
        for row in self.category.itertuples():
            dict_type_build[row[1]] = row[2]
            dict_ventilation[row[1]] = row[3]
            dict_license[row[1]] = row[4]
        self.tenants['Тип здания'] = self.tenants['Формат'].apply(lambda x: set_built_type(x, dict_type_build))
        self.tenants['Вентиляция'] = self.tenants['Формат'].apply(lambda x: set_built_type(x, dict_ventilation))
        self.tenants['Лицензия'] = self.tenants['Формат'].apply(lambda x: set_built_type(x, dict_license))
        # Папки
        name_dir = create_directory()
        print('Обработка объектов...')
        for obj in tqdm(list(self.objects.iterrows())):
            name = obj[1]['Адрес']
            area = obj[1]['Площадь']
            cost = obj[1]['Арендная ставка']
            build_type = 0 if obj[1]['Тип здания'] == 'Жилое' else 1
            vent_type = 0 if obj[1]['Вентиляция'] == 'нет' else 1
            lic_type = 0 if obj[1]['Лицензия на алкоголь'] == 'нет' else 1
            location = obj[1]['Округ']
            obj_tenants = search_tenants(self.tenants)
            obj_tenants['Номер объекта'] = obj[0]
            obj_path = os.path.join(name_dir, f'{name}.xls'.replace('/', '-'))
            save_excel(obj_tenants, obj_path)
        print(f'Готово. Обработано {len(self.objects)} объектов')
        input('Нажмите Enter для выхода')

    def mailing(self, test_mode=None):
        # Рассылка электронных писем по созданным спискам
        # Загрузка списков рассылки
        # Определение папки
        name_dir = select_directory()
        if test_mode:
            if not os.path.exists(os.path.join(name_dir, 'Тестовая рассылка')):
                os.mkdir(os.path.join(name_dir, 'Тестовая рассылка'))
        files = glob.glob(os.path.join(name_dir, '*.xls'))
        # Формирование списков
        mailing_dict = {}
        for file in files[:]:
            obj = pd.read_excel(file, index_col='#')
            for row in obj.iterrows():
                num_obj = row[1]['Номер объекта']
                if row[0] not in mailing_dict:
                    mailing_dict[row[0]] = []
                mailing_dict[row[0]].append(num_obj)
        # Подключение к почтовому серверу
        if not test_mode:
            print('Подключение к серверу')
            server = smtplib.SMTP_SSL(self.host, self.port)
            server.login(self.username, self.password)
            print('Отправка писем')
        # Проверка лог файлов
        path_log = os.path.join(name_dir, 'Отчет о рассылке.log')
        if os.path.exists(path_log):
            with open(path_log, 'r') as f:
                log_file = json.load(f)
        else:
            log_file = {}
        duplicate_emails = []
        # Загрузка шаблонов inja
        env = Environment(loader=FileSystemLoader(self.path_to_templates), trim_blocks=True, lstrip_blocks=True)
        template_plain = env.get_template('template.txt')
        template_html = env.get_template('template.html')
        # Формирование писем и отправка
        for id_tenant, id_objects in tqdm(mailing_dict.items()):
            objects_email = self.objects.loc[id_objects, :]
            tenant_email = self.tenants.loc[id_tenant, :]
            # Формирование шаблонов plain и html
            # Приветствие
            # name = tenant_email['Имя']
            name = None
            if type(name) is str:
                greeting = ', '.join(['Добрый день', name]) + '!'
            else:
                greeting = 'Добрый день!'
            if len(id_objects) > 1:
                greeting = ' '.join([greeting,
                                     'Представляем Вашему вниманию интересные предложения на рынке коммерческой недвижимости.'])
                ending = 'Подробная информация по представленным объектам во вложении.'
            else:
                greeting = ' '.join([greeting, 'Представляем Вашему вниманию торговое помещение.'])
                ending = 'Подробная информация по объекту во вложении.'
            # Рендер писем
            plain = template_plain.render(hello=greeting, end=ending, objects_list=objects_email, user=self.user)
            html = template_html.render(hello=greeting, end=ending, objects_list=objects_email, user=self.user,
                                        company=self.company)
            # Список файлов
            files_to_attach = objects_email['Адрес'].apply(
                lambda x: os.path.join('Презентации', x.replace('/', '-') + '.pdf'))

            # Формирование email
            if len(id_objects) > 1:
                subject = 'Предложения по аренде коммерческих помещений'
            else:
                subject = 'Предложение по аренде торгового помещения'
            to_emails = [x.strip() for x in tenant_email['Email'].split(',')]
            cc_emails = []
            bcc_emails = []
            emails = to_emails + cc_emails + bcc_emails

            msg = EmailMessage()
            msg['Subject'] = subject
            msg['From'] = self.from_addr
            msg['To'] = ', '.join(to_emails)
            msg["Cc"] = ', '.join(cc_emails)
            msg['Bcc'] = ', '.join(bcc_emails)
            msg.set_content(plain)
            msg.add_alternative(html, 'html')
            msg.add_header('List-Unsubscribe', f"<mailto:{self.company['unsubscribe_email']}>")
            for path in files_to_attach:
                with open(path, 'rb') as file_attach:
                    msg.add_attachment(file_attach.read(),
                                       maintype='application', subtype='pdf',
                                       filename=f'{os.path.basename(path)}')
            msg_text = msg.as_string()
            # Отправка email
            # Проверка на повторную рассылку
            if str(id_tenant) not in log_file:
                if not test_mode:
                    r = server.sendmail(self.from_addr, emails, msg_text)
                else:
                    r = {}
                    with open(os.path.join(name_dir, 'Тестовая рассылка', f'{id_tenant}.eml'), 'w') as f:
                        f.write(msg_text)
                log_file[id_tenant] = r
                with open(path_log, 'w') as f:
                    json.dump(log_file, f)
            else:
                duplicate_emails.append(f'{self.tenants.loc[id_tenant, "Email"]} уже отправлен')
        print('\n'.join(duplicate_emails))
        print('Всё готово')
        input('Нажмите Enter для выхода')
