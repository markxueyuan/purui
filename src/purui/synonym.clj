(ns purui.synonym
  (:require [purui.io :as io]
            [clojure.string :as string]
            [clojure.java.io :as javaio]))

(def speech-synonyms
  {"n" "名词"
"nr" "人名"
"nr1" "人名"
"nr2" "人名"
"nrj" "人名"
"nrf" "人名"
"ns" "地名"
"nsf" "地名"
"nt" "专有名词"
"nz" "专有名词"
"nl" "名词"
"ng" "名词"
"nw" "名词"
"t" "时间词"
"tg" "时间词"
"s" "方位词"
"f" "方位词"
"v" "动词"
"vd" "动词"
"vn" "动词"
"vshi" "动词"
"vyou" "动词"
"vf" "动词"
"vx" "动词"
"vi" "动词"
"vl" "动词"
"vg" "动词"
"a" "形容词"
"ad" "形容词"
"an" "形容词"
"ag" "形容词"
"al" "形容词"
"b" "其他"
"bl" "其他"
"z" "状态词"
"r" "代词"
"rr" "代词"
"rz" "代词"
"rzt" "代词"
"rzs" "代词"
"rzv" "代词"
"ry" "代词"
"ryt" "代词"
"rys" "代词"
"ryv" "代词"
"rg" "代词"
"m" "数量词"
"mq" "数量词"
"q" "数量词"
"qv" "数量词"
"qt" "数量词"
"d" "副词"
"p" "其他"
"pba" "其他"
"pbei" "其他"
"c" "其他"
"cc" "其他"
"u" "其他"
"uzhe" "其他"
"ule" "其他"
"uguo" "其他"
"ude1" "其他"
"ude2" "其他"
"ude3" "其他"
"usuo" "其他"
"udeng" "其他"
"uyy" "其他"
"udh" "其他"
"uls" "其他"
"uzhi" "其他"
"ulian" "其他"
"e" "其他"
"y" "其他"
"o" "其他"
"h" "其他"
"k" "其他"
"x" "其他"
"xx" "其他"
"xu" "其他"
"w" "其他"
"wkz" "其他"
"wky" "其他"
"wyz" "其他"
"wyy" "其他"
"wj" "其他"
"ww" "其他"
"wt" "其他"
"wd" "其他"
"wf" "其他"
"wn" "其他"
"wm" "其他"
"ws" "其他"
"wp" "其他"
"wb" "其他"
"wh" "其他"
"en" "其他"
"i" "成语"
"j" "简称"
"l" "常用语"})

(defn han
  [k entry]
  (let [synm (speech-synonyms (k entry) (k entry))]
    (assoc entry k synm)))

(defn keyword->brand
  [file coll]
  (let [maps (io/csv-to-map file)
        kw (fn [entry] (or (get entry :keywords) (get entry :匹配关键词) (get entry :keyword)))
        kw (fn [entry] (string/replace (kw entry) #"\"" ""))
        brand (fn [entry] (get maps (kw entry) "unkown"))]
    (map #(assoc % :brand (brand %)) coll)))


(defn categorize
  [coll category-file output-file]
  (let [cat-map (io/csv-to-map category-file)
        writer1 (javaio/writer output-file)
        writer2 (javaio/writer output-file :append true)
        col (map name (keys (first coll)))
        func (fn
               [entry]
               (let [col (keys entry)
                     value (vals entry)]
                 (when-let [cat (get cat-map (:word entry))]
                   (io/write-csv-quoted-by-row (conj value cat) writer2))))]
    (println "Filter out data having categories...")
    (io/write-csv-quoted-by-row (conj col "category") writer1)
    (.close writer1)
    (dorun (map func coll))
    (.close writer2)))

























