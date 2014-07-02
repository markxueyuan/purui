(ns purui.data-download
  (:require [purui.io :as io]
            [purui.synonym :as syn]
            [purui.html-extracter :refer [*fast*] :as extr]
            [clojure.string :as string]))

(defn split-keyword
  [entry]
  (when-let [kw (or (:keywords entry) (:匹配关键字 entry))]
    (let [ws (string/split kw #",")
          f (fn [k] (when (k entry) k))
          k (or (f :keywords) (f :匹配关键字))
          others (dissoc entry k)]
      (map #(into {k %} others) ws))))

(defn utility
  [coll brand-file]
    (->> coll
         (map extr/extract-html)
         (map split-keyword)
         flatten
         (syn/keyword->brand brand-file)))

(defn- select-55
  [sql func]
  (io/lazy-read-mysql :host "192.168.2.55"
                      :db "purui_data"
                      :user "dev"
                      :password "dev@yjxd.com"
                      :sql sql
                      :result-set-fn func))

#_(defn- select-55
  [sql func]
  (io/lazy-read-mysql :host "localhost"
                      :db "purui_data"
                      :user "root"
                      :password "othniel"
                      :sql sql
                      :result-set-fn func))



(defn news-to-csv
  [start-day end-day brand-file output-file & {:keys [extract fast]
                                    :or {fast false}}]
  (let [s1 "select * from news left join news_html on news.id=news_html.id"
        s2 "select * from news left join news_text on news.id=news_text.id"
        s3 (str " where publish_date >= '" start-day
                "' and publish_date <= '" end-day "'")
        sql (if extract (str s1 s3) (str s2 s3))
        func (fn [rs] (io/write-csv-quoted
                       (if extract
                         (binding [*fast* fast]
                           (utility rs brand-file))
                         (utility rs brand-file))
                       output-file))]
    (select-55 sql func)))

(defn weibo-to-csv
  [start-day end-day brand-file output-file]
  (let [s1 "select * from weibo"
        s3 (str " where publish_date >= '" start-day
                "' and publish_date <= '" end-day "'")
        sql (str s1 s3)
        func (fn [rs] (io/write-csv-quoted
                       (utility rs brand-file)
                       output-file))]
    (select-55 sql func)))

;(news-to-csv "2014-6-1" "2014-6-30" "D:/data/keywords_brand.csv" "D:/data/news_data.csv")

(weibo-to-csv "2014-6-1" "2014-6-30" "D:/data/keywords_brand.csv" "D:/data/weibo_data.csv")
