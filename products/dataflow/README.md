# Cloud Dataflow

Cloud Dataflow の学習用のリソース。

Apache Beam をベースにした Dataflow 2.x 系をベースに実施。


## 初期設定

### Google Cloud Platform 上で動かす場合の設定

* プロジェクト, 課金, 関連プロダクトのAPIの有効化
* サービスアカウントのJSONキーの取得
* 以下環境変数の設定
    * `GOOGLE_APPLICATION_CREDENTIALS` : サービスアカウントのJSONファイルへのパス
    * `BUCKET` : 入出力対象ファイルを配置するGCSバケットのパス(`gs://BUCKET_NAME`)


### GCP/ローカル共通

* Python 2.7 で `virtualenv` で仮想環境を作成
* `pip install google-cloud-dataflow` でライブラリをダウンロード



## References

* Official Documents
    * [Product overview](https://cloud.google.com/dataflow/)
    * [Document](https://cloud.google.com/dataflow/docs/)
