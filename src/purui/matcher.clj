(ns purui.matcher
  (:require [purui.io :as io])
  (:import java.util.regex.Pattern))

(defn reg-match
  [coll reg-file output-file]
  (let [reg-map (io/csv-to-map reg-file)
        func (fn [entry]
              (let [brand (get entry :brand)
                    string (get reg-map brand)
                    regex (when string (Pattern/compile string))
                    matches (when regex
                              (cond (:text entry) (re-seq regex (:text entry))
                                    (:title entry) (re-seq regex (:title entry))))
                    parts (-> (select-keys entry [:brand :source_name :host_name])
                              (assoc :publish_date (str (:publish_date entry))))
                    results (map #(assoc parts :regex %) matches)]
                results))]
        (-> (map func coll)
            flatten
            (io/write-csv-quoted output-file))))


(reg-match (io/lazy-read-csv-head-on "D:/data/news_data.csv")
           "D:/data/brand_regex.csv"
           "D:/data/news_match")

