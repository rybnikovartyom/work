import pandas as pd
import requests
import configparser
import logging
from datetime import timedelta, datetime
from connections import MsSql


def get_campaign_heads_from_google(url):
    """Get the list of created autocall campaigns from GoogleDoc"""
    google_heads = pd.read_csv(url)
    google_heads['date'] = pd.to_datetime(google_heads['date'], format='%d.%m.%Y')
    return google_heads


def load_heads(heads):
    """Load new campaigns' heads to SQL Server
       Return True if some heads were loaded"""
    srv_logist_conn = MsSql('srv-logist-rc')
    try:
        loaded_heads = srv_logist_conn.get_data('select distinct campaign_id '
                                                'from calls_from_zvonok_head', return_type='t')['campaign_id'].to_list()
        heads_for_load = heads.query('campaign_id not in @loaded_heads')
        if heads_for_load.shape[0] != 0:
            srv_logist_conn.modify_data(query='INSERT INTO calls_from_zvonok_head '
                                              '(campaign_id,date,script,source, client_id, processed_ind) '
                                              'values(?,?,?,?,?,?)',
                                        rows=[tuple(row)+('N',) for row in heads_for_load.values])
            return True
        else:
            return False
    except Exception:
        return False


def get_campaigns_for_load():
    srv_logist_conn = MsSql('srv-logist-rc')
    campaigns = srv_logist_conn.get_data("select distinct campaign_id,date "
                                         "from calls_from_zvonok_head "
                                         "where processed_ind=N'N'")
    return campaigns


def get_cmapaign_url(api_key, campaign_id, campaign_date, page):
    """Create url for http-query with params"""
    date_from = campaign_date - timedelta(days=7)
    date_to = campaign_date + timedelta(days=7, hours=23, minutes=59, seconds=59)
    url = f'https://zvonok.com/manager/cabapi_external/api/v1/phones/all_calls/?public_key={api_key}' \
          f'&campaign_id={campaign_id}&page={page}' \
          f'&from_created_date={date_from}&to_created_date={date_to}'.replace(' ', '%20')
    return url


def response_encoding(resp):
    """Response encode function"""
    if resp is None:
        return -1
    elif 'да' in resp.lower():
        return 1
    elif 'нет' in resp.lower():
        return 0
    else:
        pass


def get_campaign_details(api_key, campaign_id, campaign_date):
    """Get campaign details from API. Return a list of dicts."""
    date_from = campaign_date - timedelta(days=7)
    date_to = campaign_date + timedelta(days=7, hours=23, minutes=59, seconds=59)
    calls_from_campaign = []
    session = requests.Session()
    for page in range(1, 1000):
        url_calls = get_cmapaign_url(api_key, campaign_id, campaign_date, page)
        calls_response = session.get(url_calls)
        if calls_response.status_code == 200:
            if len(calls_response.json()):
                calls_from_campaign.extend(calls_response.json())
            else:
                break
        else:
            print(f'Error response: {calls_response.text}')
            break
    session.close()

    return calls_from_campaign


def load_campaign_details(calls_from_campaign, campaign_id):
    """Load campain details to SQL Server. Return length of loaded array."""
    if len(calls_from_campaign):
        calls_for_load = [(campaign_id,
                           row['phone'].replace('+', ''),
                           -1 if row['dial_status'] is None else row['dial_status'],
                           row['status'],
                           response_encoding(row['user_choice']),
                           row['updated'],
                           0 if row['duration'] is None else row['duration'],
                           float(0 if row['cost'] is None else row['cost']))
                          for row in calls_from_campaign]
        srv_logist_conn = MsSql('srv-logist-rc')
        srv_logist_conn.modify_data(query='INSERT INTO calls_from_zvonok (campaign_id,phone,'
                                          'dial_status,status,user_choice,date,duration,cost) '
                                          'values(?,?,?,?,?,?,?,?)',
                                    rows=calls_for_load)
        srv_logist_conn.execute_query(query=f"UPDATE calls_from_zvonok_head SET processed_ind=N'Y' "
                                            f"WHERE campaign_id={campaign_id}")
        srv_logist_conn.execute_query(query=f"insert into dbo.calls "
                                            f"select d.phone,d.date,h.source,h.script,d.user_choice,h.client_id "
                                            f"from [dbo].[calls_from_zvonok_head] h "
                                            f"join [dbo].[calls_from_zvonok] d "
                                            f"on h.campaign_id=d.campaign_id "
                                            f"WHERE h.campaign_id={campaign_id}")

    else:
        pass

    return len(calls_from_campaign)


def etl_zvonok_info():
    logging.basicConfig(level=logging.INFO, filename="fc_log.log", filemode="a",
                        format="%(asctime)s: [%(levelname)s] - [%(module)s] %(message)s")

    config = configparser.ConfigParser()
    config.read('./config.ini')

    # объявляем константы:
    # путь к гугл-файлу с id кампаний
    sheet_url = config['zvonok']['google_doc_url']
    # ключ для подключения к сервису по API
    public_key = config['zvonok']['api_key']
    # получаем перечень кампаний из гугл файла
    campaigns_heads_source = get_campaign_heads_from_google(sheet_url)
    # записываем заголовки новых кампаний на SQL Server
    campaigns_heads_load = load_heads(campaigns_heads_source)
    # получаем необработанные пары ид кампании-дата для запроса деталей
    campains_for_load = get_campaigns_for_load()
    # если есть кампании для запроса деталей, поочередно запрашиваем и записываем данные на SQL Server,
    # иначе выходим из программы с записью в логе
    if len(campains_for_load):
        for campaign in campains_for_load:
            calls_data = get_campaign_details(public_key, campaign.get('campaign_id'), campaign.get('date'))
            loaded_rows = load_campaign_details(calls_data, campaign.get('campaign_id'))
            logging.info(f'For campaign {campaign.get("campaign_id")} loaded {loaded_rows} rows.')
    else:
        logging.info('No campaigns for load')
