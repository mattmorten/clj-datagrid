(ns datagrid.messaging)





(defn register-route [type f]
  (swap! atom assoc type f))


