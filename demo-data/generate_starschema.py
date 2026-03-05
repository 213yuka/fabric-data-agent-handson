"""
Fabric Data Agent デモ用データ生成スクリプト（スタースキーマ）
コントソ精密工業株式会社 - 産業用センサー・計測機器メーカー

生成テーブル:
  - DimDate（日付ディメンション）
  - DimProduct（製品ディメンション）
  - DimCustomer（顧客ディメンション）
  - FactSales（売上ファクト）
  - FactProduction（生産ファクト）
  - FactInventorySnapshot（在庫スナップショット）
  - FactQualityInspection（品質検査ファクト）
"""

import csv
import random
from datetime import datetime, timedelta, date
import os

random.seed(42)

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

WEEKDAY_JP = ['月曜日', '火曜日', '水曜日', '木曜日', '金曜日', '土曜日', '日曜日']
MONTH_JP = ['', '1月', '2月', '3月', '4月', '5月', '6月',
            '7月', '8月', '9月', '10月', '11月', '12月']


# ============================================================
# DimDate
# ============================================================
def generate_dim_date():
    rows = []
    d = date(2025, 1, 1)
    end = date(2026, 3, 31)
    while d <= end:
        fy = d.year if d.month >= 4 else d.year - 1
        fq_month = (d.month - 4) % 12
        fq = f'FQ{fq_month // 3 + 1}'
        rows.append({
            'DateKey': int(d.strftime('%Y%m%d')),
            'FullDate': d.isoformat(),
            'Year': d.year,
            'Month': d.month,
            'MonthName': MONTH_JP[d.month],
            'Quarter': f'Q{(d.month - 1) // 3 + 1}',
            'DayOfWeek': WEEKDAY_JP[d.weekday()],
            'IsWeekend': d.weekday() >= 5,
            'FiscalYear': fy,
            'FiscalQuarter': fq,
        })
        d += timedelta(days=1)
    return rows


# ============================================================
# DimProduct
# ============================================================
PRODUCTS_RAW = [
    ('PRD-001', '高精度リニアセンサー LS-500', 'センサー', 185000, 92000, 14, 0.35, '2022-04-01'),
    ('PRD-002', '産業用温度センサー TS-200', 'センサー', 45000, 22000, 7, 0.12, '2021-06-15'),
    ('PRD-003', '圧力トランスミッター PT-300', 'センサー', 128000, 64000, 10, 0.45, '2023-01-10'),
    ('PRD-004', '光学式変位計 OD-800', '計測機器', 320000, 160000, 21, 1.20, '2022-09-01'),
    ('PRD-005', 'デジタルマイクロメーター DM-100', '計測機器', 78000, 38000, 5, 0.55, '2020-03-20'),
    ('PRD-006', '三次元測定機 CMM-3000', '計測機器', 4500000, 2250000, 60, 850.00, '2023-07-01'),
    ('PRD-007', '画像検査装置 VI-1000', '検査装置', 2800000, 1400000, 45, 120.00, '2022-11-15'),
    ('PRD-008', 'レーザーマーカー LM-600', '加工装置', 1650000, 825000, 30, 65.00, '2024-02-01'),
    ('PRD-009', 'PLC制御ユニット CU-400', '制御機器', 95000, 47000, 10, 2.80, '2021-08-10'),
    ('PRD-010', 'サーボモーター SM-750', '制御機器', 210000, 105000, 12, 8.50, '2023-03-15'),
    ('PRD-011', 'タッチパネルディスプレイ TD-120', '制御機器', 145000, 72000, 8, 3.20, '2022-05-20'),
    ('PRD-012', '産業用ロボットアーム RA-2000', 'ロボティクス', 6500000, 3250000, 90, 180.00, '2024-06-01'),
    ('PRD-013', '協働ロボット CR-500', 'ロボティクス', 3200000, 1600000, 60, 35.00, '2025-01-15'),
    ('PRD-014', 'AGV自動搬送車 AGV-100', 'ロボティクス', 4800000, 2400000, 75, 450.00, '2024-09-01'),
    ('PRD-015', 'IoTゲートウェイ GW-50', 'IoTデバイス', 68000, 34000, 5, 0.85, '2023-11-01'),
    ('PRD-016', 'ワイヤレスセンサーノード WSN-30', 'IoTデバイス', 35000, 17000, 3, 0.08, '2024-04-15'),
    ('PRD-017', 'エッジコンピューティングユニット EC-200', 'IoTデバイス', 248000, 124000, 15, 4.50, '2025-03-01'),
    ('PRD-018', '振動解析モジュール VA-150', 'センサー', 156000, 78000, 12, 0.65, '2024-07-20'),
    ('PRD-019', '超音波厚さ計 UT-400', '計測機器', 92000, 46000, 8, 1.10, '2023-05-10'),
    ('PRD-020', '赤外線サーモグラフィ IR-900', '検査装置', 890000, 445000, 20, 5.80, '2024-11-01'),
]

def generate_dim_product():
    rows = []
    for i, (pid, name, cat, price, cost, lead, weight, launch) in enumerate(PRODUCTS_RAW, 1):
        rows.append({
            'ProductKey': i,
            'ProductID': pid,
            'ProductName': name,
            'Category': cat,
            'UnitPrice': price,
            'UnitCost': cost,
            'ManufacturingLeadTimeDays': lead,
            'WeightKg': weight,
            'LaunchDate': launch,
        })
    return rows

# 逆引き用
PRODUCT_KEY_MAP = {p[0]: i for i, p in enumerate(PRODUCTS_RAW, 1)}
PRODUCT_PRICE_MAP = {p[0]: p[3] for p in PRODUCTS_RAW}


# ============================================================
# DimCustomer
# ============================================================
CUSTOMERS_RAW = [
    ('CUS-001', '瑞穂モーターズ株式会社', '自動車製造', '中部', '愛知県', 70000, '2018-04-01', 'S', '田中太郎'),
    ('CUS-002', '朝凪電装株式会社', '自動車部品', '中部', '愛知県', 45000, '2019-01-15', 'S', '田中太郎'),
    ('CUS-003', '蒼天エレクトロニクス株式会社', '電機メーカー', '関西', '大阪府', 60000, '2018-06-01', 'S', '山本花子'),
    ('CUS-004', '暁峰電機株式会社', '電機メーカー', '関東', '東京都', 30000, '2020-03-10', 'A', '佐藤健一'),
    ('CUS-005', '碧空精機株式会社', 'FA機器', '関東', '山梨県', 8500, '2019-07-20', 'A', '佐藤健一'),
    ('CUS-006', '翠光産業株式会社', '空圧機器', '関東', '東京都', 20000, '2021-02-01', 'A', '鈴木美咲'),
    ('CUS-007', '瑞光電子工業株式会社', '電子部品', '関西', '京都府', 75000, '2019-11-15', 'S', '山本花子'),
    ('CUS-008', '昇龍重工株式会社', '重工業', '関東', '東京都', 80000, '2020-08-01', 'A', '佐藤健一'),
    ('CUS-009', '紅葉重機株式会社', '重工業', '関西', '兵庫県', 36000, '2021-04-10', 'B', '山本花子'),
    ('CUS-010', '彩雲テクノロジー株式会社', '電子部品', '関東', '東京都', 28000, '2022-01-15', 'B', '鈴木美咲'),
    ('CUS-011', '暁ロボティクス株式会社', 'ロボット', '九州', '福岡県', 15000, '2020-05-20', 'A', '高橋誠'),
    ('CUS-012', '銀嶺精密株式会社', 'FA機器', '関西', '京都府', 28000, '2019-09-01', 'A', '山本花子'),
    ('CUS-013', '双葉物流機器株式会社', 'マテハン', '関西', '大阪府', 12000, '2022-06-15', 'B', '山本花子'),
    ('CUS-014', '雷光電動機株式会社', 'モーター', '関西', '京都府', 110000, '2021-08-01', 'S', '山本花子'),
    ('CUS-015', '星明フォトニクス株式会社', '光学機器', '中部', '静岡県', 5500, '2023-01-10', 'B', '田中太郎'),
    ('CUS-016', '天領計測株式会社', '計測制御', '関東', '東京都', 18000, '2020-11-20', 'A', '佐藤健一'),
    ('CUS-017', '和泉パーツ工業株式会社', '自動車部品', '中部', '愛知県', 35000, '2022-03-01', 'A', '田中太郎'),
    ('CUS-018', '北斗空調工業株式会社', '空調機器', '関西', '大阪府', 90000, '2021-12-15', 'S', '山本花子'),
    ('CUS-019', '明星電工株式会社', '電線・光学', '関西', '大阪府', 40000, '2023-04-01', 'B', '山本花子'),
    ('CUS-020', '碧海精密機器株式会社', '精密機器', '中部', '長野県', 75000, '2022-09-10', 'A', '高橋誠'),
    ('CUS-021', '曙光電機株式会社', '電機メーカー', '関東', '東京都', 28000, '2023-06-01', 'B', '鈴木美咲'),
    ('CUS-022', '極東精工株式会社', 'ベアリング', '関東', '東京都', 95000, '2021-05-15', 'A', '佐藤健一'),
    ('CUS-023', '紫雲窯業株式会社', 'セラミック部品', '関西', '京都府', 80000, '2020-02-01', 'S', '山本花子'),
    ('CUS-024', '若草精密工業株式会社', '精密機器', '中部', '愛知県', 40000, '2022-07-20', 'B', '田中太郎'),
    ('CUS-025', '晴嵐光機株式会社', '光学機器', '関東', '東京都', 20000, '2021-10-01', 'A', '佐藤健一'),
    ('CUS-026', '鶴翼機械株式会社', '機械部品', '関東', '東京都', 13000, '2023-08-15', 'B', '鈴木美咲'),
    ('CUS-027', '大和冶金株式会社', '鉄鋼', '関東', '東京都', 106000, '2022-11-01', 'A', '佐藤健一'),
    ('CUS-028', '青葉自動車株式会社', '自動車製造', '関東', '東京都', 37000, '2024-01-15', 'B', '佐藤健一'),
    ('CUS-029', '初音自動車工業株式会社', '自動車製造', '中国', '広島県', 23000, '2023-11-01', 'B', '高橋誠'),
    ('CUS-030', '飛鳥エンジニアリング株式会社', '重工業', '関東', '東京都', 29000, '2024-03-01', 'B', '鈴木美咲'),
]

def generate_dim_customer():
    rows = []
    for i, (cid, name, ind, reg, pref, emp, start, rank, rep) in enumerate(CUSTOMERS_RAW, 1):
        rows.append({
            'CustomerKey': i,
            'CustomerID': cid,
            'CustomerName': name,
            'Industry': ind,
            'Region': reg,
            'Prefecture': pref,
            'EmployeeCount': emp,
            'AccountStartDate': start,
            'CustomerRank': rank,
            'SalesRep': rep,
        })
    return rows

CUSTOMER_KEY_MAP = {c[0]: i for i, c in enumerate(CUSTOMERS_RAW, 1)}
CUSTOMER_RANK_MAP = {c[0]: c[7] for c in CUSTOMERS_RAW}

# 顧客の注文頻度ウェイト（ランクに基づく）
CUSTOMER_WEIGHTS = []
for cid, _, _, _, _, _, _, rank, _ in CUSTOMERS_RAW:
    w = {'S': 5, 'A': 3, 'B': 1}[rank]
    CUSTOMER_WEIGHTS.extend([cid] * w)


# ============================================================
# FactSales
# ============================================================
def date_to_key(d):
    return int(d.strftime('%Y%m%d'))

def generate_fact_sales():
    rows = []
    start = date(2025, 1, 1)
    end = date(2026, 3, 31)
    product_ids = list(PRODUCT_KEY_MAP.keys())
    statuses = ['出荷済'] * 7 + ['処理中', '受注確定', 'キャンセル']

    for key in range(1, 1501):
        order_date = start + timedelta(days=random.randint(0, (end - start).days))
        cust_id = random.choice(CUSTOMER_WEIGHTS)
        prod_id = random.choice(product_ids)
        unit_price = PRODUCT_PRICE_MAP[prod_id]

        # 高額商品は少量、低額商品は多量
        if unit_price >= 2000000:
            qty = random.choices([1, 2, 3, 5], weights=[40, 30, 20, 10])[0]
        elif unit_price >= 500000:
            qty = random.choices([1, 2, 3, 5, 10], weights=[15, 20, 25, 25, 15])[0]
        else:
            qty = random.choices([1, 2, 3, 5, 10, 20, 50, 100],
                                 weights=[5, 8, 12, 20, 25, 15, 10, 5])[0]

        # 数量割引
        if qty >= 50:
            discount = 0.15
        elif qty >= 20:
            discount = 0.10
        elif qty >= 10:
            discount = 0.05
        else:
            discount = 0.0

        selling_price = int(unit_price * (1 - discount))
        total = selling_price * qty
        status = random.choice(statuses)

        ship_date_key = None
        if status == '出荷済':
            ship_d = order_date + timedelta(days=random.randint(3, 14))
            ship_date_key = date_to_key(ship_d)

        rows.append({
            'SalesKey': key,
            'OrderID': f'ORD-{key:05d}',
            'OrderDateKey': date_to_key(order_date),
            'ShipDateKey': ship_date_key if ship_date_key else '',
            'CustomerKey': CUSTOMER_KEY_MAP[cust_id],
            'ProductKey': PRODUCT_KEY_MAP[prod_id],
            'Quantity': qty,
            'UnitSellingPrice': selling_price,
            'TotalAmount': total,
            'DiscountRate': discount,
            'Status': status,
            'DeliveryRegion': random.choice(['関東', '中部', '関西', '九州', '東北', '北海道', '中国']),
        })
    return rows


# ============================================================
# FactProduction
# ============================================================
def generate_fact_production():
    rows = []
    start = date(2025, 1, 1)
    end = date(2026, 3, 31)
    product_ids = list(PRODUCT_KEY_MAP.keys())
    lines = ['ライン-A', 'ライン-B', 'ライン-C', 'ライン-D', 'ライン-E']
    factories = ['本社工場', '第二工場', '九州工場']
    shifts = ['日勤', '夜勤']

    for key in range(1, 2001):
        prod_date = start + timedelta(days=random.randint(0, (end - start).days))
        prod_id = random.choice(product_ids)
        line = random.choice(lines)
        factory = random.choice(factories)
        shift = random.choice(shifts)
        planned = random.choices([50, 100, 200, 500, 1000], weights=[10, 25, 30, 25, 10])[0]

        # 高額商品は生産数少なめ
        if PRODUCT_PRICE_MAP[prod_id] >= 2000000:
            planned = random.choices([5, 10, 20, 50], weights=[30, 35, 25, 10])[0]

        # 歩留まり（5%の確率で不良多発）
        if random.random() < 0.05:
            yield_rate = random.uniform(0.75, 0.90)
        else:
            yield_rate = random.uniform(0.95, 0.998)

        actual = int(planned * yield_rate)
        defect = planned - actual
        work_h = round(random.uniform(4.0, 12.0), 1)
        downtime = random.randint(15, 180) if random.random() < 0.1 else random.randint(0, 10)

        rows.append({
            'ProductionKey': key,
            'ProductionRecordID': f'MFG-{key:05d}',
            'ProductionDateKey': date_to_key(prod_date),
            'ProductKey': PRODUCT_KEY_MAP[prod_id],
            'Factory': factory,
            'ProductionLine': line,
            'Shift': shift,
            'PlannedQuantity': planned,
            'ActualQuantity': actual,
            'DefectQuantity': defect,
            'YieldRate': round(yield_rate, 4),
            'WorkHours': work_h,
            'DowntimeMinutes': downtime,
        })
    return rows


# ============================================================
# FactInventorySnapshot
# ============================================================
def generate_fact_inventory():
    rows = []
    key = 1
    warehouses = ['本社倉庫', '東京物流センター', '大阪物流センター', '九州物流センター']
    snapshot_date = date(2026, 3, 31)
    snapshot_key = date_to_key(snapshot_date)

    for prod_id, price in PRODUCT_PRICE_MAP.items():
        for wh in warehouses:
            if price >= 2000000:
                stock = random.randint(0, 20)
                safety = random.choice([3, 5, 10])
            else:
                stock = random.randint(0, 500)
                safety = random.choices([10, 20, 50, 100], weights=[30, 30, 25, 15])[0]

            if stock < safety:
                status = '在庫不足'
            elif stock <= safety * 5:
                status = '適正'
            else:
                status = '過剰在庫'

            last_receipt = snapshot_date - timedelta(days=random.randint(1, 60))

            rows.append({
                'InventoryKey': key,
                'SnapshotDateKey': snapshot_key,
                'ProductKey': PRODUCT_KEY_MAP[prod_id],
                'WarehouseName': wh,
                'CurrentStock': stock,
                'SafetyStock': safety,
                'StockStatus': status,
                'LastReceiptDateKey': date_to_key(last_receipt),
                'ShelfLocation': f'{random.choice("ABCDEFGH")}-{random.randint(1,20):02d}-{random.randint(1,5):02d}',
            })
            key += 1
    return rows


# ============================================================
# FactQualityInspection
# ============================================================
def generate_fact_quality():
    rows = []
    start = date(2025, 1, 1)
    end = date(2026, 3, 31)
    product_ids = list(PRODUCT_KEY_MAP.keys())
    insp_types = ['受入検査', '工程内検査', '最終検査', '出荷前検査']
    results_pool = ['合格'] * 8 + ['条件付合格', '不合格']
    defect_cats = ['外観不良', '寸法不良', '機能不良', '動作不良', '組立不良', '塗装不良', '電気特性不良']
    inspectors = ['山田一郎', '佐々木次郎', '中村美紀', '木村浩二', '渡辺恵子']

    for key in range(1, 801):
        insp_date = start + timedelta(days=random.randint(0, (end - start).days))
        prod_id = random.choice(product_ids)
        insp_type = random.choice(insp_types)
        lot_size = random.choices([10, 50, 100, 200, 500], weights=[15, 25, 30, 20, 10])[0]
        sample_size = max(1, int(lot_size * random.uniform(0.05, 0.20)))
        result = random.choice(results_pool)

        if result == '不合格':
            defect_cat = random.choice(defect_cats)
            defect_count = random.randint(1, sample_size)
        elif result == '条件付合格':
            defect_cat = random.choice(defect_cats)
            defect_count = random.randint(1, max(1, sample_size // 5))
        else:
            defect_cat = 'なし'
            defect_count = 0

        rows.append({
            'QualityKey': key,
            'InspectionID': f'QA-{key:05d}',
            'InspectionDateKey': date_to_key(insp_date),
            'ProductKey': PRODUCT_KEY_MAP[prod_id],
            'InspectionType': insp_type,
            'LotSize': lot_size,
            'SampleSize': sample_size,
            'Result': result,
            'DefectCategory': defect_cat,
            'DefectCount': defect_count,
            'Inspector': random.choice(inspectors),
        })
    return rows


# ============================================================
# 書き出し
# ============================================================
def write_csv(filename, rows):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f'  {filename}: {len(rows)} rows')


if __name__ == '__main__':
    print('Generating star schema data...')

    dim_date = generate_dim_date()
    dim_product = generate_dim_product()
    dim_customer = generate_dim_customer()
    fact_sales = generate_fact_sales()
    fact_production = generate_fact_production()
    fact_inventory = generate_fact_inventory()
    fact_quality = generate_fact_quality()

    write_csv('DimDate.csv', dim_date)
    write_csv('DimProduct.csv', dim_product)
    write_csv('DimCustomer.csv', dim_customer)
    write_csv('FactSales.csv', fact_sales)
    write_csv('FactProduction.csv', fact_production)
    write_csv('FactInventorySnapshot.csv', fact_inventory)
    write_csv('FactQualityInspection.csv', fact_quality)

    print('\nDone! All CSV files generated.')
