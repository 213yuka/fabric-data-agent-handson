# Fabric Data Agent ハンズオンガイド  製造業データで体験する自然言語データ分析

## Data Agent とは？

Microsoft Fabric の **データエージェント（Data Agent）** は、企業のデータに対して**自然言語で質問するだけで、AI が SQL を自動生成して回答を返す**機能です。

```
あなた：「今月の売上トップ 5 製品を教えて」

Data Agent：（裏で SQL を自動生成 → 実行 → 結果を整形）

 | 順位 | 製品名               | 売上合計      |
 |------|----------------------|---------------|
 | 1    | 高精度レーザーセンサー | ¥12,450,000  |
 | 2    | 産業用協働ロボット    | ¥11,200,000  |
 | ...  | ...                  | ...           |
```

**SQL の知識は一切不要**。現場のマネージャーや経営層が、自分の言葉でデータに問いかけられます。

---

## このリポジトリについて

製造業の架空企業「**コントソ精密工業株式会社**」のデモデータを使って、Data Agent の機能を体験できるハンズオン環境です。

### 含まれるデータ

| テーブル | 内容 | 行数 |
|---|---|---|
| DimDate | 日付マスタ（2025/1〜2026/3） | 455 行 |
| DimProduct | 製品マスタ（センサー、計測機器、ロボティクス等） | 20 行 |
| DimCustomer | 顧客マスタ（30 社、ランク S/A/B） | 30 行 |
| FactSales | 売上トランザクション | ~1,600 行 |
| FactProduction | 生産記録（歩留まり、ダウンタイム等） | ~2,100 行 |
| FactInventorySnapshot | 在庫スナップショット | ~80 行 |
| FactQualityInspection | 品質検査記録（合格/不合格/不良カテゴリ） | ~860 行 |

### データモデル（スタースキーマ）

```
                    ┌──────────────┐
                    │  DimProduct  │
                    │  （製品）     │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────┴───────┐    ┌──────────────┐
│ DimCustomer  │────│  FactSales   │────│   DimDate    │
│ （顧客）      │    │  （売上）     │    │  （日付）     │
└──────────────┘    └──────────────┘    └──────┬───────┘
                                               │
                    ┌──────────────┐            │
                    │FactProduction│────────────┤
                    │ （生産）      │            │
                    └──────────────┘            │
                                               │
                    ┌──────────────┐            │
                    │FactInventory │────────────┤
                    │（在庫snapshot）│            │
                    └──────────────┘            │
                                               │
                    ┌──────────────┐            │
                    │ FactQuality  │────────────┘
                    │ （品質検査）   │
                    └──────────────┘
```

4 つのファクトテーブルが製品・顧客・日付の 3 つのディメンションと結合し、**売上 × 生産 × 在庫 × 品質の横断分析**が可能です。

---

## セットアップ手順

### 前提条件

- Microsoft Fabric の容量（F2 以上）が利用可能であること
- Fabric ワークスペースの管理者権限があること

### Step 1 — Lakehouse の作成とデータアップロード

1. Fabric ポータルでワークスペースを開く
2. 「新規」→「Lakehouse」を作成
3. `demo-data/` フォルダ内の **7 つの CSV ファイル**を Lakehouse の Files にアップロード
4. 各 CSV ファイルを右クリック →「Load to Table」で Delta テーブルに変換

### Step 2 — セマンティックモデルの作成

Data Agent の回答精度を高めるために、**セマンティックモデル**の構築を推奨します。

#### なぜセマンティックモデルが重要なのか

| セマンティックモデルなし | セマンティックモデルあり |
|---|---|
| Data Agent がテーブル間の関係を推測する | リレーションシップが明示的に定義されている |
| 「売上合計」の定義が曖昧 | DAX メジャーで計算ロジックが事前定義されている |
| JOIN ミスが起きやすい | 正しい結合が保証される |

#### 構築方法

**方法 A: Fabric ポータルで手動作成**

1. ワークスペースで「新規」→「セマンティックモデル」を作成
2. Lakehouse の SQL エンドポイントからテーブルを追加
3. リレーションシップを設定（下記参照）
4. DAX メジャーを追加

**方法 B: 定義ファイルからデプロイ（上級者向け）**

`semantic-model/model_directlake_v2.bim` を REST API 経由でデプロイできます。

#### リレーションシップ（8 本）

| 多側テーブル | 結合列 | → | 1 側テーブル | 結合列 |
|---|---|---|---|---|
| FactSales | OrderDateKey | → | DimDate | DateKey |
| FactSales | ProductKey | → | DimProduct | ProductKey |
| FactSales | CustomerKey | → | DimCustomer | CustomerKey |
| FactProduction | ProductionDateKey | → | DimDate | DateKey |
| FactProduction | ProductKey | → | DimProduct | ProductKey |
| FactInventorySnapshot | SnapshotDateKey | → | DimDate | DateKey |
| FactInventorySnapshot | ProductKey | → | DimProduct | ProductKey |
| FactQualityInspection | ProductKey | → | DimProduct | ProductKey |

#### 主要 DAX メジャー（32 本、抜粋）

| メジャー名 | 内容 |
|---|---|
| 売上合計 | `SUM(FactSales[TotalAmount])` |
| 有効売上合計 | キャンセルを除外した売上 |
| 粗利額 / 粗利率 | 売上 − 原価 / 売上対比 |
| 平均歩留まり率 | `AVERAGE(FactProduction[YieldRate])` |
| 計画達成率 | 実績数量 ÷ 計画数量 |
| 在庫充足率 | 安全在庫以上の SKU 比率 |
| 合格率 / 不合格率 | 品質検査の結果比率 |

全メジャーの定義は `semantic-model/model_directlake_v2.bim` に含まれています。

### Step 3 — Data Agent の作成

1. ワークスペースで「新規」→「データエージェント」を作成
2. データソースとして **Step 2 で作成したセマンティックモデル**を選択
3. AI インストラクションに以下を設定：

```
あなたはコントソ精密工業株式会社（架空の産業用機器メーカー）のデータアナリストです。
社内の売上・生産・在庫・品質データを分析して質問に回答してください。

【データの前提知識】
- 顧客ランク: S（年間取引 1 億円以上）、A（3,000 万〜1 億円）、B（3,000 万円未満）
- 会計年度は 4 月始まり（FiscalYear / FiscalQuarter を使用）
- 歩留まり率（YieldRate）は 0.0〜1.0 の小数（95% 以上が正常）
- 在庫ステータスは現在庫数と安全在庫数の比較で決定
- 金額はすべて日本円（JPY）

【回答ルール】
- 日本語で、簡潔かつ構造的に回答する
- 金額にはカンマ区切りと ¥ を付ける
- 可能な場合は前月比・前年比を添える
```

4. 「公開」をクリックして Data Agent を有効化

---

## 試してみよう — Data Agent に聞いてみる質問例

### 基本（ウォームアップ）

```
今月の売上合計はいくら？
```

### クロステーブル分析

```
顧客ランク S の顧客の、直近 3 ヶ月の月別売上推移を教えて
```

### 製造業ならではの分析

```
歩留まり率が 90% を下回った生産記録を、製品名・工場・ラインごとに見せて
```

### 意思決定支援

```
在庫不足の製品で、かつ直近 1 ヶ月に受注が多いものはどれ？優先補充リストを作って
```

### 品質トレンド

```
不良カテゴリ別の月次推移を出して。増加傾向のカテゴリはある？
```

### 経営視点

```
地域別・製品カテゴリ別の売上構成比を教えて
```

---

## ファイル構成

```
demo-data/                          # デモ用データ（CSV）
├── DimDate.csv                     # 日付ディメンション
├── DimProduct.csv                  # 製品ディメンション
├── DimCustomer.csv                 # 顧客ディメンション
├── FactSales.csv                   # 売上ファクト
├── FactProduction.csv              # 生産ファクト
├── FactInventorySnapshot.csv       # 在庫スナップショットファクト
├── FactQualityInspection.csv       # 品質検査ファクト
└── generate_starschema.py          # データ生成スクリプト

semantic-model/
└── model_directlake_v2.bim         # セマンティックモデル定義（DirectLake）
```

---

## テーブル定義の詳細

<details>
<summary>DimDate（日付ディメンション）</summary>

| 列名 | 型 | 説明 |
|---|---|---|
| DateKey | INT | YYYYMMDD 形式のキー |
| FullDate | DATE | 日付 |
| Year | INT | 年 |
| Month | INT | 月 |
| MonthName | STRING | 月名（1月〜12月） |
| Quarter | STRING | 四半期（Q1〜Q4） |
| DayOfWeek | STRING | 曜日 |
| IsWeekend | BOOLEAN | 週末フラグ |
| FiscalYear | INT | 会計年度（4 月始まり） |
| FiscalQuarter | STRING | 会計四半期 |

</details>

<details>
<summary>DimProduct（製品ディメンション）</summary>

| 列名 | 型 | 説明 |
|---|---|---|
| ProductKey | INT | サロゲートキー |
| ProductID | STRING | 製品 ID（PRD-001 等） |
| ProductName | STRING | 製品名 |
| Category | STRING | カテゴリ（センサー/計測機器/検査装置/制御機器/ロボティクス/IoT デバイス） |
| UnitPrice | INT | 標準単価（円） |
| UnitCost | INT | 原価（円） |
| ManufacturingLeadTimeDays | INT | 製造リードタイム（日） |
| WeightKg | FLOAT | 重量（kg） |
| LaunchDate | DATE | 発売日 |

</details>

<details>
<summary>DimCustomer（顧客ディメンション）</summary>

| 列名 | 型 | 説明 |
|---|---|---|
| CustomerKey | INT | サロゲートキー |
| CustomerID | STRING | 顧客 ID（CUS-001 等） |
| CustomerName | STRING | 顧客名 |
| Industry | STRING | 業種 |
| Region | STRING | 地域（関東/中部/関西/九州/中国） |
| Prefecture | STRING | 都道府県 |
| EmployeeCount | INT | 従業員数 |
| AccountStartDate | DATE | 取引開始日 |
| CustomerRank | STRING | ランク（S/A/B） |
| SalesRep | STRING | 担当営業 |

</details>

<details>
<summary>FactSales（売上ファクト）</summary>

| 列名 | 型 | 説明 |
|---|---|---|
| SalesKey | INT | サロゲートキー |
| OrderID | STRING | 受注 ID |
| OrderDateKey | INT | 受注日キー → DimDate |
| ShipDateKey | INT | 出荷日キー → DimDate |
| CustomerKey | INT | → DimCustomer |
| ProductKey | INT | → DimProduct |
| Quantity | INT | 数量 |
| UnitSellingPrice | INT | 販売単価 |
| TotalAmount | INT | 合計金額 |
| DiscountRate | FLOAT | 割引率 |
| Status | STRING | ステータス（受注確定/処理中/出荷済/キャンセル） |
| DeliveryRegion | STRING | 納品先地域 |

</details>

<details>
<summary>FactProduction（生産ファクト）</summary>

| 列名 | 型 | 説明 |
|---|---|---|
| ProductionKey | INT | サロゲートキー |
| ProductionRecordID | STRING | 生産記録 ID |
| ProductionDateKey | INT | 生産日キー → DimDate |
| ProductKey | INT | → DimProduct |
| Factory | STRING | 工場名 |
| ProductionLine | STRING | 製造ライン |
| Shift | STRING | シフト（日勤/夜勤） |
| PlannedQuantity | INT | 計画数量 |
| ActualQuantity | INT | 実績数量 |
| DefectQuantity | INT | 不良数量 |
| YieldRate | FLOAT | 歩留まり率 |
| WorkHours | FLOAT | 作業時間（h） |
| DowntimeMinutes | INT | 設備停止時間（min） |

</details>

<details>
<summary>FactInventorySnapshot（在庫スナップショットファクト）</summary>

| 列名 | 型 | 説明 |
|---|---|---|
| InventoryKey | INT | サロゲートキー |
| SnapshotDateKey | INT | スナップショット日キー → DimDate |
| ProductKey | INT | → DimProduct |
| WarehouseName | STRING | 倉庫名 |
| CurrentStock | INT | 現在庫数 |
| SafetyStock | INT | 安全在庫数 |
| StockStatus | STRING | 在庫ステータス（在庫不足/適正/過剰在庫） |
| LastReceiptDateKey | INT | 最終入庫日キー → DimDate |
| ShelfLocation | STRING | 棚番 |

</details>

<details>
<summary>FactQualityInspection（品質検査ファクト）</summary>

| 列名 | 型 | 説明 |
|---|---|---|
| QualityKey | INT | サロゲートキー |
| InspectionID | STRING | 検査 ID |
| InspectionDateKey | INT | 検査日キー → DimDate |
| ProductKey | INT | → DimProduct |
| InspectionType | STRING | 検査種別（受入検査/工程内検査/最終検査/出荷前検査） |
| LotSize | INT | ロットサイズ |
| SampleSize | INT | サンプル数 |
| Result | STRING | 検査結果（合格/条件付合格/不合格） |
| DefectCategory | STRING | 不良カテゴリ |
| DefectCount | INT | 不良数 |
| Inspector | STRING | 検査担当者 |

</details>

---

## FAQ

### Q: 日本語の質問に対応していますか？

A: はい。Data Agent は日本語の自然言語を理解し、日本語で回答を返します。テーブルや列名が英語でも、AI インストラクションで日本語対応を指示できます。

### Q: データを自社のものに差し替えられますか？

A: はい。`demo-data/` の CSV を自社データの CSV に差し替え、セマンティックモデルのテーブル定義を合わせれば、同じ手順で Data Agent を構築できます。

---

## 参考リンク

- [Microsoft Fabric Data Agent ドキュメント](https://learn.microsoft.com/ja-jp/fabric/data-science/concept-data-agent)
- [Microsoft Fabric Lakehouse ドキュメント](https://learn.microsoft.com/ja-jp/fabric/data-engineering/lakehouse-overview)
- [DirectLake セマンティックモデル](https://learn.microsoft.com/ja-jp/fabric/fundamentals/direct-lake-overview)

---

## ライセンス

このリポジトリのデモデータおよびスクリプトは、学習・検証目的で自由にご利用いただけます。  
データはすべて架空のものであり、実在の企業・個人とは一切関係ありません。
