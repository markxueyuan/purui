(ns purui.word-seg
  (:import (java.util List)
           (org.ansj.domain Term)
           (org.ansj.splitWord.analysis BaseAnalysis)
           (org.ansj.splitWord.analysis ToAnalysis)
           (org.ansj.recognition NatureRecognition)
           (org.ansj.library UserDefineLibrary))
  (:require [purui.synonym :as syn]
            [purui.io :as io]
            [purui.html-extracter :refer [*fast*] :as extr]
            [clojure.string :as string]))



(defn mapper
  "looted from Hu's code"
  [^Term item]
  {:word (.getName item)
   :nature (.natureStr (.getNatrue item))})

(defn factory
  "basically copied from Hu"
  [string]
  (let [terms (ToAnalysis/parse string)
        recog (new NatureRecognition terms)
        _ (.recognition recog)]
    terms))

(defn word-seg
  [col-key entry]
  (when-let [string (col-key entry)]
    (let [seg (factory string)
          mapped (map mapper seg)]
      (into entry {:word-seg mapped}))))

;(word-seg :c {:a 12345 :b "hahahah" :c "我作为一个中国人是无比自豪的！"})

(defn define-library
  [white-list]
  (for [x white-list]
    (let [[word nature frequency] x]
      (UserDefineLibrary/insertWord word nature frequency))))


(def white-list [["沙夫豪森" "selfdefined" 1000
                  "超薄机芯" "selfdefined" 1000
                  "潜水表" "selfdefined" 1000
                  "镂空" "selfdefined" 1000
                  "陀飞轮" "selfdefined" 1000
                  "超长动力" "selfdefined" 1000
                  "动物灵感" "selfdefined" 1000
                  "三问表" "selfdefined" 1000
                  "月相" "selfdefined" 1000
                  "两地时" "selfdefined" 1000
                  "计时码表" "selfdefined" 1000]])


(defn factory-with-dic
  [string]
  (let [_ (define-library white-list)
        terms (ToAnalysis/parse string)
        recog (new NatureRecognition terms)
        _ (.recognition recog)]
    terms))


;(time (factory-with-dic "陀飞轮是一个好东西"))


(defn unwind
  [entry]
  (let [pivot (dissoc entry :word-seg)
        word-seg (get entry :word-seg)
        func #(into pivot %)]
    (map func word-seg)))

(defn split-keyword
  [entry]
  (when-let [kw (or (:keywords entry) (:匹配关键字 entry))]
    (let [ws (string/split kw #",")
          f (fn [k] (when (k entry) k))
          k (or (f :keywords) (f :匹配关键字))
          others (dissoc entry k)]
      (map #(into {k %} others) ws))))

(defn utility
  [coll seg-col brand-file & cols]
  (let [news [:匹配关键词 :媒介名称 :日期 :来源]
        news2 [:keywords :host_name :publish_date :source_name]
        weibo [:匹配关键词 :日期 :性别 :省]
        weibo2 [:keywords :publish_date :gender]
        default [:keywords :publish_date]
        head (set (keys (first coll)))
        cols (cond cols (conj cols :word-seg :brand)
                   (not (nil? (get head :性别))) (conj weibo :word-seg :brand)
                   (not (nil? (get head :gender))) (conj weibo2 :word-seg :brand)
                   (not (nil? (get head :媒介名称))) (conj news :word-seg :brand)
                   (not (nil? (get head :host_name))) (conj news2 :word-seg :brand)
                   :else (conj default :word-seg :brand))]
    (->> coll
         (map extr/extract-html)
         (map #(word-seg seg-col %))
         (map #(select-keys % cols))
         (map unwind)
         flatten
         (map split-keyword)
         flatten
         (filter #(> (count (:word %)) 1))
         (map #(syn/han :nature %))
         (syn/keyword->brand brand-file)
         )))

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


(defn news-to-seg
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
                           (utility rs :html brand-file))
                         (utility rs :text brand-file))
                       output-file))]
    (select-55 sql func)))

(defn weibo-to-seg
  [start-day end-day brand-file output-file]
  (let [s1 "select * from weibo"
        s3 (str " where publish_date >= '" start-day
                "' and publish_date <= '" end-day "'")
        sql (str s1 s3)
        func (fn [rs] (io/write-csv-quoted
                       (utility rs :title brand-file)
                       output-file))]
    (select-55 sql func)))


#_(time (io/lazy-read-mysql :db "purui_data"
                          :host "192.168.2.55"
                          :user "dev"
                          :password "dev@yjxd.com"
                 :sql "select * from news left join news_text on news.id=news_text.id where publish_time >= '2014-6-20' and publish_time < '2014-6-21'"
                 :result-set-fn (fn [rs] (io/write-csv-quoted rs "D:/data/111.csv"))))

#_(time (io/lazy-read-mysql :db "purui"
                 :sql "select * from news"
                 :result-set-fn (fn [rs] (io/write-csv-quoted (binding [*fast* true] (utility rs :title)) "D:/data/testtime4.csv"))))

(news-to-seg "2014-6-1" "2014-6-30" "D:/data/keywords_brand.csv" "D:/data/segstext2.csv")


;(weibo-to-seg "2014-6-1" "2014-6-30" "D:/data/keywords_brand.csv" "D:/data/segstext6.csv")
