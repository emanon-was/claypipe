(ns claypipe.stream
  (:import [clojure.lang Sequential])
  (:use [clojure.core.async :only [go chan <! <!! >! >!! close!]]))

(defrecord Stream [channel])
(defrecord EOS [])
(defn eos? [_]
  (instance? EOS _))

(defprotocol IStreamable
  (from [_]))

(extend-type Sequential
  IStreamable
  (from [lst]
    (let [out (chan 100)]
      (go (doseq [_ lst] (>! out _))
          (>! out (EOS.)))
      (Stream. out))))

(defprotocol IStream
  (where  [s f])
  (select [s f])
  (collect [s])
  (stdout  [s]))


(extend-type Stream
  IStream
  (where [s f]
    (let [in (:channel s) out (chan 100)]
      (go (loop [_ (<! in)]
            (if (eos? _)
              (do (>! out _) (close! in))
              (do (if (f _) (>! out _))
                  (recur (<! in))))))
      (Stream. out)))
  (select [s f]
    (let [in (:channel s) out (chan 100)]
      (go (loop [_ (<! in)]
            (if (eos? _)
              (do (>! out _) (close! in))
              (do (>! out (f _))
                  (recur (<! in))))))
      (Stream. out)))
  (collect [s]
    (let [in (:channel s) coll (transient [])]
      (loop [_ (<!! in)]
        (if (eos? _)
          (do (close! in)
              (persistent! coll))
          (do (conj! coll _)
              (recur (<!! in)))))))
  (stdout [s]
    (let [in (:channel s)]
      (loop [_ (<!! in)]
        (if (eos? _)
          (do (close! in))
          (do (println _)
              (recur (<!! in))))))))


;; (-> (from (range 30))
;;     (where odd?)
;;     (select
;;      #(cond (= (rem % 15) 0) "FizzBuzz"
;;             (= (rem % 3)  0) "Fizz"
;;             (= (rem % 5)  0) "Buzz"
;;             :else %))
;;     collect)

;; [1 "Fizz" "Buzz" 7 "Fizz" 11 13 "FizzBuzz" 17 19 "Fizz" 23 "Buzz" "Fizz" 29]

