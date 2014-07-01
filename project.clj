(defproject purui "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"local" ~(str (.toURI (java.io.File. "maven_repository")))
                 "mvn-repo" "http://ansjsun.github.io/mvn-repo/"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/data.json "0.2.3"]
                 [org.clojure/data.csv "0.1.2"]
                 [lein-light-nrepl "0.0.10"]
                 [clj-time "0.6.0"]
                 [org.ansj/ansj_seg "0.9"]
                 [clj-excel "0.0.1"]
                 [clojure-csv/clojure-csv "2.0.1"]
                 [org.clojure/java.jdbc "0.3.3"]
                 [mysql/mysql-connector-java "5.1.25"]
                 [net.htmlparser.jericho/jericho-html "3.1"]
                 [org.clojars.sids/htmlcleaner "2.1"]
                 [commons-lang "2.5"]
                 ]
  :repl-options {:nrepl-middleware [lighttable.nrepl.handler/lighttable-ops]}
  :jvm-opts ["-Xmx1g"])
