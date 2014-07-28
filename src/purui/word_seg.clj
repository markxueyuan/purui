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


(defn utility2
  [coll seg-col cols]
  (let [news [:匹配关键词 :媒介名称 :日期 :来源]
        news2 [:keywords :host_name :publish_date :source_name]
        weibo [:匹配关键词 :日期 :性别 :省]
        weibo2 [:keywords :publish_date :weibo_user_name :gender :location]
        default [:keywords :publish_date]
        head (set (keys (first coll)))
        cols (cond cols (conj cols :word-seg :brand)
                   (not (nil? (get head :性别))) (conj weibo :word-seg :brand)
                   (not (nil? (get head :gender))) (conj weibo2 :word-seg :brand)
                   (not (nil? (get head :媒介名称))) (conj news :word-seg :brand)
                   (not (nil? (get head :host_name))) (conj news2 :word-seg :brand)
                   :else (conj default :word-seg :brand))]
    (->> coll
         (map #(word-seg seg-col %))
         (map #(select-keys % cols))
         (map unwind)
         flatten
         (filter #(> (count (:word %)) 1))
         (map #(syn/han :nature %)))))

;(utility2 (io/lazy-read-csv-head-on "D:/data/news_data_haha") :text nil)


(defn to-seg
  [input-file output-file seg-col & cols]
  (-> (io/lazy-read-csv-head-on input-file)
      (utility2 seg-col cols)
      (io/write-csv-quoted output-file)))

;(to-seg "D:/data/news_data_haha" "D:/data/segstext2.csv" :text)

