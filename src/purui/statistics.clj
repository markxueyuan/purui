(ns purui.statistics
  (:require [purui.pivot-table :as pt]
            [purui.io :as io]
            [clojure.java.io :as javaio]
            [clojure.string :as string]))


(defn word-frequency
  [coll mid-file output-file & cols]
  (let [mid-file2 (str mid-file 2)
        func (fn [p pivots]
               (let [s (group-by #(select-keys % pivots) p)]
                 (map #(assoc (first %) :count (reduce + (map read-string (map :count (second %))))) s)))]


    (println "First round accumulation begins, check file " mid-file)

    (let [coll (map #(select-keys % cols) coll)
          pars (partition-all 20000 coll)
          func (fn [p]
                 (let [s (frequencies p)]
                   (map #(assoc (first %) :count (second %)) s)))]
      (io/write-csv-quoted (func (first pars)) mid-file)
      (doseq [p (rest pars)]
        (io/write-csv-quoted (func p) mid-file :append true)))

    (println "Second round accumulation begins, check file " mid-file2)

    (let [coll (io/lazy-read-csv-head-on mid-file)
          pars (partition-all 200000 coll)
          pivots (keys (dissoc (first (io/lazy-read-csv-head-on mid-file)) :count))]
      (io/write-csv-quoted (func (first pars) pivots) mid-file2)
      (doseq [p (rest pars)]
        (io/write-csv-quoted (func p pivots) mid-file2 :append true)))
    ;(javaio/delete-file mid-file)

    (println "Results are to be distributed to every brand")

    (let [coll (io/lazy-read-csv-head-on mid-file2)
          brands (do
                   (println "In gathering brands......")
                   (distinct (map :brand (io/lazy-read-csv-head-on mid-file2))))
          pairs (reduce #(assoc %1 %2 (javaio/writer (str mid-file2 "_" %2) :append true)) {} brands)
          a (atom #{})]
      (println "The brands are " brands)
      (println "Data are distributing to each brand...")
      (doseq [q coll]
        (let [brand (:brand q)
              col (keys q)
              value (vals q)]
          (if-not (get @a brand)
            (do (swap! a conj brand)
              (io/write-csv-quoted-by-row (map name col) (get pairs brand))
              (io/write-csv-quoted-by-row value (get pairs brand)))
            (io/write-csv-quoted-by-row value (get pairs brand)))))
      (doseq [w (vals pairs)]
        (.close w))
      ;(javaio/delete-file mid-file2)

      (println "Last step. Output statistics for each brand")

      (doseq [f @a]
        (let [file (str mid-file2 "_" f)
              output-file (str output-file "_" f ".csv")
              coll (io/lazy-read-csv-head-on file)
              pivots (keys (dissoc (first (io/lazy-read-csv-head-on file)) :count))]
          (-> (func coll pivots)
              (io/write-csv-quoted output-file))
          ;(javaio/delete-file file)
          )))

    (println "All jobs are done!")))




;(word-frequency (c) "D:/data/mid-file.csv" "D:/data/news" :brand :publish_date :word :nature)


