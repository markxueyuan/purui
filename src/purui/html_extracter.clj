(ns purui.html-extracter
  (:import [org.htmlcleaner HtmlCleaner];htmlcleaner
           [org.apache.commons.lang StringEscapeUtils]
           [net.htmlparser.jericho Source TextExtractor Renderer];jericho
           ))

(defn parse-page
  [page-src]
  (try
   (when page-src
     (let [cleaner (new HtmlCleaner)]
       (doto (.getProperties cleaner) ;; set HtmlCleaner properties
         (.setOmitComments true)
         (.setPruneTags "script,style"))
       (when-let [node (.clean cleaner page-src)]
         (-> node
            (.getText)
            (str)
            (StringEscapeUtils/unescapeHtml)))))
   (catch Exception e
     (println "Error when parsing"))))

(def ^:dynamic *fast* false)

(defn extract-html
  [entry]
  (if-let [html (:html entry)]
    (let [parts (dissoc entry :html)]
        (if-not *fast*
          (assoc parts :html (parse-page html));htmlcleaner
          (assoc parts :html (str (Renderer. (Source. html))))));jericho
    entry))
