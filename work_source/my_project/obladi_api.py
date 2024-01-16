import requests
import configparser
from connections import Rms
from datetime import date
import logging

session = requests.Session()

config = configparser.ConfigParser()
config.read('config.ini')

obladi_auth = (config['obladi']['user'], config['obladi']['token'])


def load_items_to_obladi():
    """This function get items for load to marketplace from Oracle database,
       post items to marketplace via API and update posted product_id in database."""
    rms_conn = Rms()
    items_for_load = rms_conn.get_data('select * from v_y_obladi_item where product_id is null', 'j')
    # Prepare JSON-like file with items for load via marketplace API
    goods = [{"product": item['ITEM_DESC'],
              "short_description": item['ITEM_PARENT'],
              "price": item['PRICE'],
              "parent_product_id": "0",
              "category_ids": item['ID_CATEGORY'],
              "main_category": item['ID_CATEGORY'],
              "status": "A",
              "amount": "0",
              "product_code": item['ITEM'],
              "product_type": "P",
              "company_id": "57",
              "usergroup_ids": "9",
              "image_pairs": [{'detailed': {'image_path': i}} for i in item['REST_IMAGES_ADDR'].split(',')]
              if item['REST_IMAGES_ADDR'] is not None else None,
              "main_pair": {"detailed": {"image_path": item.get('MAIN_IMAGE_ADDR')}},
              "promo_text": """"Условия работы:по договору,без НДС включен,возврат/обмен по браку,предоплата или 
                                оплата по счету. Отсрочка платежа индивидуально.Доставка бесплатно по Новосибирску и 
                                пригороду в течении 2 раб.дн.,возможно-на следующий день.Доставка в НСО транспортной 
                                компанией, за счёт покупателя. Минимальная сумма заказа от 1000 руб.""",
              "full_description": ((str(item['DESCRIPTION']) + '\n' if item['DESCRIPTION'] is not None else '') +
                                   ('Состав: ' + str(item['COMPOSITION']) + '\n'
                                    if item['COMPOSITION'] is not None else '') +
                                   ('Срок хранения: ' + str(item['SHELF_LIFE']) + ' сутки.\n'
                                    if str(item['SHELF_LIFE']) == '1' else
                                    'Срок хранения: ' + str(item['SHELF_LIFE']) + ' суток.\n'
                                    if item['SHELF_LIFE'] is not None else '') +
                                   ('Условия хранения: ' + str(item['CONDITIONS']) + '\n'
                                    if item['CONDITIONS'] is not None else '') +
                                   ('Пищевая ценность на 100 г:\n' +
                                    'белки: ' + (str(item['PROTES']) if item['PROTES'] else '0') + ' г, ' +
                                    'жиры: ' + (str(item['FAT']) if item['FAT'] else '0') + ' г, ' +
                                    'углеводы: ' + (str(item['CARBONES']) if item['CARBONES'] else '0') + ' г.\n '
                                    if item['PROTES'] is not None and
                                       item['FAT'] is not None and
                                       item['CARBONES'] is not None else '') +
                                   ('Калорийность: ' + str(item['CALORY']) + ' ккал.'
                                    if item['CALORY'] is not None else '')),
              "min_qty": item['QUANT'],
              "qty_step": item['QUANT'],
              "tax_ids": {"tax_id": "9"},
              "show_master_products_only": "False",
              "units_in_product": 1,
              "show_price_per_x_units": item['QUANT'],
              "unit_name": "шт" if item['STANDARD_UOM'] == "EA" else "кг"} for item in items_for_load]

    # Load items one-by-one
    load_list = []
    for good in goods:
        # Post item to marketplace
        res = session.post('https://obladi.ru/api/2.0/products/', auth=obladi_auth, json=good)
        item_pair = (res.json()['product_id'], good['short_description'])
        # Update marketplace product_id in Oracle database
        rms_conn.change_data('update y_obladi_item set product_id=:1,last_update_date=sysdate where item=:2', item_pair)
        # Fill list of posted items
        load_list.append(item_pair)

    return load_list


def update_items_obladi_full():
    # Select items for load to marketplace from Oracle database
    rms_conn = Rms()
    items_for_update = rms_conn.get_data('select * from v_y_obladi_item where product_id is not null', 'j')
    stocks = get_stocks()
    for obj in items_for_update:
        for item in stocks:
            if obj['ITEM_PARENT'] == item['ITEM_PARENT']:
                obj.update(item)
    goods = [{"product_id": item['PRODUCT_ID'],
              "product": item['ITEM_DESC'],
              "short_description": item['ITEM_PARENT'],
              "price": item['PRICE'],
              "amount": item.get('SOH', '0'),
              "usergroup_ids": "9",
              # "image_pairs": [{'detailed': {'image_path': i}} for i in item['REST_IMAGES_ADDR'].split(',')]
              # if item['REST_IMAGES_ADDR'] is not None else None,
              # "main_pair": {"detailed": {"image_path": item.get('MAIN_IMAGE_ADDR')}},
              "full_description": ((str(item['DESCRIPTION']) + '\n' if item['DESCRIPTION'] is not None else '') +
                                   ('Состав: ' + str(item['COMPOSITION']) + '\n'
                                    if item['COMPOSITION'] is not None else '') +
                                   ('Срок хранения: ' + str(item['SHELF_LIFE']) + ' сутки.\n'
                                    if str(item['SHELF_LIFE']) == '1' else
                                    'Срок хранения: ' + str(item['SHELF_LIFE']) + ' суток.\n'
                                    if item['SHELF_LIFE'] is not None else '') +
                                   ('Условия хранения: ' + str(item['CONDITIONS']) + '\n'
                                    if item['CONDITIONS'] is not None else '') +
                                   ('Пищевая ценность на 100 г:\n' +
                                    'белки: ' + (str(item['PROTES']) if item['PROTES'] else '0') + ' г, ' +
                                    'жиры: ' + (str(item['FAT']) if item['FAT'] else '0') + ' г, ' +
                                    'углеводы: ' + (str(item['CARBONES']) if item['CARBONES'] else '0') + ' г.\n '
                                    if item['PROTES'] is not None and
                                       item['FAT'] is not None and
                                       item['CARBONES'] is not None else '') +
                                   ('Калорийность: ' + str(item['CALORY']) + ' ккал.'
                                    if item['CALORY'] is not None else '')),
              "min_qty": item['QUANT'],
              "qty_step": item['QUANT'],
              "show_price_per_x_units": item['QUANT']} for item in items_for_update]
    update_list = []
    for good in goods:
        # Post item to marketplace
        res = session.put(f'https://obladi.ru/api/2.0/products/{good["product_id"]}', auth=obladi_auth, json=good)
        item_pair = [(res.json()['product_id'], good['short_description'])]
        # Update marketplace product_id in Oracle database
        rms_conn.change_data('update y_obladi_item set product_id=:1,last_update_date=sysdate where item=:2', item_pair)
        # Fill list of posted items
        update_list.append(item_pair)

    return update_list


# обновление остатков - раз в час
# обновление цен и изображений
def get_data_from_1c(http_query, auth_1c, entities_dict=None):
    """Функция для получения данных из 1С по протоколу Odata
    В параметре entities_dict при необходимости указываются поля, которые нужно получить
    в виде списка или кортежа, а также словаря, если нужно присвоить новые названия для полученных полей"""

    http_session = requests.Session()
    http_response = http_session.get(url=http_query, headers={'Authorization': auth_1c})
    data = []
    if entities_dict is None:
        data = http_response.json()['value']
    elif isinstance(entities_dict, dict):
        data = [{entities_dict[key]: doc[key]
                 for key in doc.keys()
                 if key in list(entities_dict.keys())}
                for doc in http_response.json()['value']]
    elif isinstance(entities_dict, (tuple, list)):
        data = [{key: doc[key]
                 for key in doc.keys()
                 if key in entities_dict}
                for doc in http_response.json()['value']]
    else:
        pass

    http_session.close()

    return data


def get_stocks():
    stocks_map = {'ВНаличииBalance': 'stock_on_hand',
                  'ПоступитBalance': 'expected_approved',
                  'ЗаказаноBalance': 'expected_all',
                  'РезервироватьНаСкладеBalance': 'reserved',
                  'РезервироватьПоМереПоступленияBalance': 'reserved_by_receipt',
                  'КОбеспечениюBalance': 'orders_not_reserved',
                  'Номенклатура_Key': 'item_key'}
    items_map = {'Ref_Key': 'item_key',
                 'Description': 'item_desc',
                 'ум_КодРМС_ITEM': 'item'}
    stocks_url = f"AccumulationRegister_ЗапасыИПотребности/Balance(Dimensions='Номенклатура'," \
                 f"%20Period='{date.today().strftime('%Y-%m-%dT%H:%M:%S')}')?$format=json"
    items_url = "Catalog_Номенклатура?$format=json"
    stocks = get_data_from_1c(config['one_c']['base_url'] + stocks_url, config['one_c']['auth'], stocks_map)
    items = get_data_from_1c(config['one_c']['base_url'] + items_url, config['one_c']['auth'], items_map)
    for obj in stocks:
        for item in items:
            if obj['item_key'] == item['item_key']:
                obj.update(item)
    stocks = [{'ITEM_PARENT': item['item'], 'SOH': (item['stock_on_hand'] -
                                                    item['reserved'] -
                                                    item['reserved_by_receipt'])} for item in stocks]
    return stocks


def run_update_obladi():
    logging.basicConfig(level=logging.INFO, filename="fc_log.log", filemode="a",
                        format="%(asctime)s: [%(levelname)s] - [%(module)s] %(message)s")
    logging.info(f"Obladi items refresh started")
    try:
        updated = update_items_obladi_full()
        logging.info(f"{len(updated)} item_pairs were updated")
    except Exception:
        logging.error("Exception", exc_info=True)


run_update_obladi()



