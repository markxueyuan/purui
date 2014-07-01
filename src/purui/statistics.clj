(ns purui.statistics
  (:require [purui.pivot-table :as pt]
            [purui.io :as io]
            [clojure.java.io :as javaio]
            [clojure.string :as string]))


(defn word-frequency
  [coll mid-file output-file & cols]
  (let [mid-file2 (str mid-file 2)]

    ;first round accumulation

    (let [coll (map #(select-keys % cols) coll)
          pars (partition-all 20000 coll)
          func (fn [p]
                 (let [s (frequencies p)]
                   (map #(assoc (first %) :count (second %)) s)))]
      (io/write-csv-quoted (func (first pars)) mid-file)
      (doseq [p (rest pars)]
        (io/write-csv-quoted (func p) mid-file :append true)))

    ;second round accumulation

    (let [coll (io/lazy-read-csv-head-on mid-file)
          pars (partition-all 200000 coll)
          pivots (keys (dissoc (first coll) :count))
          func (fn [p]
                 (let [s (group-by #(select-keys % pivots) p)]
                   (map #(assoc (first %) :count (reduce + (map read-string (map :count (second %))))) s)))]
      (io/write-csv-quoted (func (first pars)) mid-file2)
      (doseq [p (rest pars)]
        (io/write-csv-quoted (func p) mid-file2 :append true)))
    (javaio/delete-file mid-file)

    ;distribution by brand

    (let [coll (io/lazy-read-csv-head-on mid-file2)
          pivots (keys (dissoc (first coll) :count))
          func (fn [p]
                 (let [s (group-by #(select-keys % pivots) p)]
                   (map #(assoc (first %) :count (reduce + (map read-string (map :count (second %))))) s)))
          a (atom #{})]
      (doseq [q coll]
        (let [brand (:brand q)
              f (str mid-file2 "_" brand)]
          (if-not (get @a brand)
            (do (swap! a conj brand)
              (io/write-csv-quoted [q] f))
            (io/write-csv-quoted [q] f :append true))))
      (javaio/delete-file mid-file2)

      ;accumulate each brand

      (doseq [f @a]
        (let [file (str mid-file2 "_" f)
              output-file (str output-file "_" )]
          (-> (func (io/lazy-read-csv-head-on file))
              (io/write-csv-quoted output-file))
          (javaio/delete-file file))))))

#_(word-frequency (io/lazy-read-csv-head-on "D:/data/segstext2.csv") "D:/data/news_middle.csv" "D:/data/news_stats.csv"
                :brand :publish_date :word :nature)

























(doseq  [a #{1 2 3}]
  (println (inc a)))

























(let [mid-file "D:/data/news_middle.csv"
      mid-file2 (str mid-file 2)
      coll (io/lazy-read-csv-head-on mid-file)
          pars (partition-all 200000 coll)
          pivots (keys (dissoc (first coll) :count))
          func (fn [p]
                 (let [s (group-by #(select-keys % pivots) p)]
                   (map #(assoc (first %) :count (reduce +  (map read-string (map :count (second %))))) s)))]
      (io/write-csv-quoted (func (first pars)) mid-file2)
      (doseq [p (rest pars)]
        (io/write-csv-quoted (func p) mid-file2 :append true)))


(group-by #(select-keys % [:a :b]) [{:a 2 :b 2 :c 3} {:a 2 :b 2 :c 4} {:a 5 :b 6 :c 7} {:a 5 :b 6 :c 8}])

(let [mid-file "D:/data/news_middle.csv"
      mid-file2 (str mid-file 2)
      coll (io/lazy-read-csv-head-on mid-file2)
          pivots (keys (dissoc (first coll) :count))
          func (fn [p]
                 (let [s (group-by #(select-keys % pivots) p)]
                   (map #(assoc (first %) :count (reduce + (map read-string (map :count (second %))))) s)))]
      (io/write-csv-quoted (func coll) "D:/data/news_stats.csv"))


(let [output-file "D:/data/pinpai/stats"
      mid-file2 "D:/data/news_middle.csv2"]
     ; coll (io/lazy-read-csv-head-on mid-file2)
      ;pivots (keys (dissoc (first coll) :count))

  #_(doseq [q coll]
    (let [brand (:brand q)
          f (str mid-file2 "_" brand)]
      (if-not (get @a brand)
        (do (swap! a conj brand)
          (io/write-csv-quoted [q] f))
        (io/write-csv-quoted [q] f :append true))))
  ;(javaio/delete-file mid-file2)
  (doseq [f b]
    (let [file (str mid-file2 "_" f)
          output-file (str output-file "_" )
          coll (io/lazy-read-csv-head-on file)
          pivots (keys (dissoc (first coll) :count))
          func (fn [p]
             (let [s (group-by #(select-keys % pivots) p)]
               (map #(assoc (first %) :count (reduce + (map read-string (map :count (second %))))) s)))
      a (atom #{})]
      (-> (func (io/lazy-read-csv-head-on file))
          (io/write-csv-quoted output-file))
      (javaio/delete-file file))))

(def a"卡地亚
宝格丽
梵克雅宝
蒂芙尼
香奈儿
七彩云南
百达翡丽
爱彼
江诗丹顿
朗格
宝玑
罗杰杜彼
帕玛强尼
宝珀
雅典表
法穆兰
格拉苏蒂
芝柏
瑞驰 迈迪
劳力士
万国
积家
尊皇表
萧邦表
伯爵
雅克德罗
真力时
沛纳海
欧米茄
万宝龙
宇舶表
柏莱士
百年灵
泰格豪雅
帝舵
名士
雷达
浪琴
摩凡陀
天梭
美度")

(def b (string/split a #"\n"))

