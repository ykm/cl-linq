;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; cl-linq library
;;;; LLGPL

;;;; pluggable in-memory database system, loosely derived from LINQ.
;;;;
;;;; Architecture: operators are singular, take one or more data
;;;; frames and return a data frame. Data frames store metadata,
;;;; including headers.
;;;;
;;;; Implementations of data frames can be done via subclassing
;;;; data-frame and implementing a "few good defmethods".  The example
;;;; data frame is a 2D array.
;;;;
;;;; Data-frames also include indexes, which are linked to one or more
;;;; headers. When an index exists, the hypothetical query planner
;;;; uses that & associated search algorithms to hunt down the
;;;; appropriate methods. Indicies should be reflective at run-time to
;;;; allow the query planner to discover its properties and use
;;;; them. Ideally, multiple indicies for the same key using different
;;;; access characteristics could be implemented (e.g., a hash table
;;;; for an EXISTS-style query, a binary tree for a sargable query..).
;;;;
;;;; The hypothetical & TBD query planner will exist as two layers of
;;;; optimization: layer 1 is a code *parser* of the queries in an
;;;; attempt to perform optimization on the query requested, and layer
;;;; 2 is a straight-forward optimizer examining sizes of data tables,
;;;; etc; the usual query planner in the literature.
;;;;
;;;; This library should also be sufficiently generic to support
;;;; streaming datasets and queries.
;;;;

(ql:quickload '(:cl-store
                :alexandria
                :cl-containers))

(defun map-with-index (type function first-sequence &rest sequences)
  "Maps over `function`, passing in the index to the sequence as well
as elements of the sequence. The underlying semantics are of MAP."
  (let ((counter 0))
    (apply #'map type
           #'(lambda (&rest sequences)
               (apply
                function
                (incf counter)
               sequences))
           first-sequence sequences)))

(defclass header ()
  ((canonical-name
    :reader canonical-name
    :initarg :canonical-name
    :initform nil
    :documentation "Will default to a text rendering of the index")
   (canonical-index
    :reader canonical-index
    :initarg :canonical-index
    :initform (error "must provide a canonical index")
    :documentation "This specifies the offset for the index")
   (alias-list
    :accessor alias-list
    :initarg :alias-list
    :initform nil
    :documentation "These are the values can also refer to the
    header."))
  (:documentation "This is a header object: it describes the names of
  a column, as well as the actual index of said column."))

(defmethod equalg ((a header) (b header))
  (and
   (string= (canonical-name a) (canonical-name b))
   (= (canonical-index a) (canonical-index b))))

(defmethod equalg ((a header) (b string))
  (and
   (string= (canonical-name a) b)))

(defmethod equalg ((a string) (b header))
  (and
   (string= a (canonical-name b))))

(defun equal-headers (a b)
  (loop
    for i in a
    for j in b
    do
       (if (not (equalg i j))
           (return-from equal-headers nil)))
  t)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; The data frame forms the fundamental unit of the cl-linq system.
(defclass data-frame ()
  ((headers
    :accessor headers
    :initform nil
    :initarg :headers
    :documentation "The headers, each one corresponding to each column
    in the data")
   (data
    :accessor data
    :initform (make-array '(2) :adjustable t :fill-pointer 0)
    :initarg :data
    :documentation "A remark on the data format. The `data-frame` is
    designed to be used as part of a data system with indexes applied
    across the system. Since an efficiently accessed data format
    relies on the sort method (whether in a vector or by a tree), and
    this is a *general* system, the design decision was made to keep
    data quickly accessible once the index was known - hence, the
    array format. ")
   (data-count
    :accessor data-count
    :initform nil
    :documentation "Returns the last known length of the data, as of
    the setting the data in the data-frame"))
  (:documentation "A dataframe is the data + metadata for a given
  page."))

(defmethod width ((obj data-frame))
  (length (headers obj)))

(defmethod lengthg ((obj data-frame))
  (data-count obj))

(defmethod (setf data) :after (new-value (obj data-frame))
  (setf (data-count obj) (first (array-dimensions (data obj)))))

(defmethod (setf headers) :after (new-headers (obj data-frame))
  (with-slots (headers)
      obj
      (setf headers
            (loop
              for h in new-headers
              for i from 0
                  collect
                  (etypecase h
                    (header
                     h)
                    (number
                     (make-instance 'header
                                    :canonical-name (format nil "~a" h)
                                    :canonical-index h))
                    (string
                     (make-instance 'header
                                    :canonical-name h
                                    :canonical-index i)))))))

(defgeneric cast-to-vector (source-object)
  (:documentation "Builds an array of arrays from source-object and returns
  the data and the number of rows thereof "))

(defmethod initialize-instance :after ((obj data-frame)  &key)
  (multiple-value-bind (data length)
      (cast-to-vector (data obj))
    (setf (data obj) data)
    (setf (data-count obj) length)))

;; Assumes a simple data form - not an alist, plist, or hash table.
(defun make-data-frame (data &optional headers)
  (let ((obj (make-instance 'data-frame
                            :data data)))
    (setf (headers obj)
          (if headers
              headers
              (loop for i from 0 below (length (aref (data obj) 0))
                    collect i)))
    obj))

(defmethod cast-to-vector ((obj data-frame))
  (values (data obj) (lengthg obj)))

(defmethod cast-to-vector ((list list))
  "This presumes that `list` is two dimensions."
  (let* ((length (length list))
         (data
           (make-array
            (list length)
            :adjustable t
            :fill-pointer length)))
    (loop
      for row in list
      for i from 0
      do
         (setf (aref data i) (concatenate 'vector row)))
    (values data length)))

(defmethod cast-to-vector ((array array))
  (values array (array-dimension array 0)))

;; (defun array-row-at (array subscript)
;;   (let* ((starting-point
;;           (array-row-major-index array subscript 0))
;;          (width (array-dimension array 1))
;;          (row (make-array (list width))))
;;     (loop for i from 0 below width
;;           do
;;              (setf (aref row i) (row-major-aref array (+ starting-point i))))
;;     row))


(defmethod cutout-clone ((frame data-frame))
  "A \"cutout\" clone of the data frame, with the same headers and
  width, but without the data."
  (make-instance 'data-frame
                 :headers (headers frame)
                 :width (width frame)))

(defmethod insert ((frame data-frame) (row vector) &key )
  (vector-push-extend row (data frame))
  (incf (data-count frame))
  frame)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Backing store
(defclass store ()
  ()
  (:documentation "A store is a generalized resource locator, suitable
  for accessing anything."))

(defgeneric read-store (store-resource-locator)
  (:documentation "Reads from store-resource-locator and returns a page"))
(defgeneric write-store (store-resource-locator page))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Page: backing store + in-memory + metadata. The page controls the
;;; loading of the data frame and retains information about where to
;;; reread the data if it is unloaded.

(defclass page ()
  ((first-index
    :accessor first-key
    :initform nil
    :initarg :first-key)
   (last-key
    :accessor last-key
    :initform nil
    :initarg :last-key)
   (locked-p
    :accessor locked-p
    :initform nil
    :initarg :locked-p
    :documentation "A table can set the locked bit on a page. Locked
    pages ought not be assigned to. ")
   (data
    :initform nil
    :initarg :data
    :accessor data
    :documentation "Data is either nil (unloaded) or a data-frame.")
   (backing-store
    :initform nil
    :initarg :backing-store
    :accessor backing-store))
  (:documentation "A table has multiple pages. Each page has an actual
  backing object - i.e., a file on-disk. Pages are known by the range
  of primary keys they contain, along with the actual data rows. A
  page A page can unload its data frame on request, but requests to
  read will cause a reload of the data frame."))

(defmethod initialize-instance :after ((page page) &key)
  (with-slots (data)
      page
    (etypecase data
      (data-frame
       t)
      (list
       (setf data
             (make-instance 'data-frame :data data)))
      (vector
       (setf data
             (make-instance 'data-frame :data data))))))

(defmethod data :before ((page page))
  "Automatically load up from the store if it isn't already there."
  (with-slots (data)
      page
    (unless data
      (load-page page))))

(defmethod data :around ((page page))

  (let ((result (call-next-method)))
    (when result
      (data result))))

(defmethod data-frame ((page page))
  (with-slots (data)
      page
    data))

(defmethod (setf data-frame) (new-value (page page))
  (setf (data page) new-value)
  (data-frame page))

;; Ensures that data being inserted is a data frame.
(defmethod (setf data) :around (new-data (page page))
  (when (locked-p page)
    (error "Attempting to modify a locked page."))
  (if (and new-data
           (not (typep new-data 'data-frame)))
      (call-next-method (make-data-frame new-data) page)
      (call-next-method)))

(defmethod lengthg ((obj page))
  (lengthg (data-frame obj)))

(defmethod unload-page  ((page page))
  (with-slots (data) page
    (setf data nil)))

(defmethod load-page ((page page))
  (setf (data page)
        (data (backing-store page))))

(defmethod flush-page ((obj page))
  (setf (data (backing-store obj)) obj))

(defmethod headers ((page page))
  (headers (data-frame page)))

(defun make-page (backing-store &optional data (headers nil))
  (let ((obj (make-instance 'caching-page :backing-store backing-store)))
    (when data
      (setf (data obj)
            (make-data-frame data headers)))
    obj))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; The caching logic manages loading and unloading in smart ways.



;; A kind of cache


;;; This is what can be put in a cache
(defclass cachable-item ()
  ((cached
    :accessor cached
    :initform nil)
   (cache
    :accessor cache
    :initform nil))
  (:documentation "The cachable item knows whether it is cached or
  not, and to which cache it belongs. Note that the current design only
  allows for belonging to one cache"))

(defclass cache ()
  ((size
    :accessor size
    :initform 0)
   (members
    :accessor members
    :initform (cl-containers:make-container 'cl-containers:heap-container)
    :initarg :members
    :documentation "Elements should conform to cachable-item")
   (test
       :accessor test
     :initform #'eql
     :initarg :test)
   (cache-overflow-p
    :accessor cache-overflow-p
    :initform #'(lambda (self) (> (size self) 32))
    :initarg :caching-page-overflow-p))
  (:documentation "The cache knows how large it is, keeps a list of
  its members, knows how to compare the members, and knows how to
  determine if it is overflowing (and thus eviction time)"))


(defmethod note-cached ((item cachable-item) (cache cache))
  (setf (cache item) cache)
  (setf (cached item) t))

(defmethod note-uncached ((item cachable-item) (cache cache))
;  (format t "~&~a is now uncached~%" item)
  (setf (cache item) nil)
  (setf (cached item) nil))

(defgeneric insert-cache-item (item cache)
  (:documentation "Insert cache item is intended to be only an
  insertion into members, and overridden by child classes. It should
  be repeatable without significantly changing the behavior."))
(defmethod insert-cache-item ((item cachable-item) (cache cache))
  (cl-containers:insert-item (members cache)
                             item))

(defgeneric delete-cache-item (item cache)
  (:documentation "delete cache item is intended to be only an
  deletion from members, and overridden by child classes"))
(defmethod delete-cache-item ((item cachable-item) (cache cache))
  (cl-containers:delete-item (members cache) item))

(defgeneric add-to-cache (item cache)
  (:documentation "add to cache calls insert-cache-item, which is
  expected to be implemented for a given cache type, and also handles
  general management of caches"))
(defmethod add-to-cache ((item cachable-item) (cache cache))
  (insert-cache-item item cache)
  (note-cached item cache)
  (incf (size cache)))

(defgeneric remove-from-cache (item cache)
  (:documentation "Remove from cache calls delete-cache-item, which is
  expected to be implemented for a given cache type, and also handles
  general management of caches"))
(defmethod remove-from-cache ((item cachable-item) (cache cache))
  ;; This may already have been removed.
  (when (cached item)
    (delete-cache-item item cache)
    (note-uncached item cache)
    (decf (size cache))))

(defgeneric evict-to-size (cache)
  (:documentation "Evict-to-size removes all overflowing members of cache
  based on evict-worst's choices."))
(defmethod evict-to-size ((cache cache))
  (when (funcall (cache-overflow-p cache) cache)
    (loop until (not (funcall (cache-overflow-p cache) cache))
          do
             (evict-worst cache))
    t))

(defmethod evict-worst (cache)
  :documentation "Evict-worst finds the worst item, based on
  find-worst-item's choice, unloads it, then calls delete-cache-item
  on the item.")
(defmethod evict-worst ((cache cache))
  (let ((old-item (find-worst-item cache)))

    (unload-item old-item)
    (remove-from-cache old-item cache))
  (decf (size cache)))

(defclass lru-cache (cache)
  ((members
    :accessor members
    :initform nil
    :documentation "A LRU cache has a particular insertion/deletion
    pattern.")))

(defmethod insert-cache-item ((item cachable-item) (cache lru-cache))
  ;; Remove remark of item in the history, then add it in on the top.
  (setf (members cache)
        ;; It is to be hoped that this is a nice shiny pointer-based
        ;; delete, as that will be O(n) to find item, then a quick
        ;; fiddle of the CDR to repoint to the CDDR.
        (delete item (members cache)))
  (push item (members cache)))

(defmethod delete-cache-item ((item cachable-item) (cache lru-cache))
  (setf (members cache)
        ;; It is to be hoped that this is a nice shiny pointer-based
        ;; delete, as that will be O(n) to find item, then a quick
        ;; fiddle of the CDR to repoint to the CDDR.
        (delete item (members cache))))

(defmethod access ((item cachable-item) (cache lru-cache))
  (insert-cache-item item cache))

(defmethod find-worst-item ((cache lru-cache))
  ;; In our LRU cache, the worst is the furthest back.
  (car (last (members cache))))

(defmethod data :after ((item cachable-item))
  (access item (cache item)))

(defgeneric unload-item (cachable-item)
  (:documentation "Unloads the internal backing data from memory"))

(defgeneric flush-item (cachable-item)
  (:documentation "A cachable item should occasionally flush to the
  backing store in case it gets evicted"))

;;;;;;;;;;

(defparameter *page-cache*
  (make-instance 'lru-cache
                 :test #'eql))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; The page of the caching.
(defclass caching-page (page cachable-item)
  ())

(defmethod initialize-instance :after ((page caching-page) &key)
  (add-to-cache page *page-cache*))

(defmethod unload-item ((page caching-page))
  (unload-page page))

(defmethod unload-page :after ((page caching-page))
  (remove-from-cache page *page-cache*))

(defmethod load-page :after ((page caching-page))
  (add-to-cache page *page-cache*))

(defmethod flush-cached-pages ((cache cache))
  "Flushes the currently cached pages out to the backing store."
  (loop for page in *page-cache*
        do (flush-page page)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defclass disk-store (store)
  ((filename
    :accessor filename
    :initform (error "Requires filename")
    :initarg :filename)))

(defmethod data ((store disk-store))
  (read-store store))

(defmethod (setf data) (new-value (store disk-store))
  (write-store store new-value))

(defmethod read-store ((store disk-store))
  (cl-store:restore (filename store)))

(defmethod write-store ((store disk-store) (page page))
  (cl-store:store
   (data page)
   (filename store)))

(defclass memory-store (store)
  ((data :accessor data :initform nil :initarg :data))
  (:documentation "A purely in-memory backing store. Useful for testing"))

(defmethod read-store ((store memory-store))
  (data store))

(defmethod write-store ((store memory-store) (page page))
  (setf (data store) (data page)))

(defclass url-store (store)
  ((url :accessor url :initform (error "Requires url") :initarg :url))
  (:documentation "A store backed by a url."))

(defmethod read-store ((store url-store))
  (error "Unimplemented"))

(defmethod write-store ((store url-store) (page page))
  (error "Unimplemented"))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; degenerate k-d tree partition.
;; points are composed of (x1 x2 data)
;; each range DOES NOT INTERSECT other ranges.
(defun k-d-tree (points)
  (when points
    (let*
       ((points (copy-list points))
        (sorted-points (sort points #'< :key #'car))
        (median-idx (floor (/ (length sorted-points) 2))))
     (list
      (elt points median-idx)
      (k-d-tree (subseq points 0 median-idx))
      (k-d-tree (subseq points (1+ median-idx)))))))

(defun find-nearest-key (kd-tree key)
  (when kd-tree
    (let ((node (first kd-tree)))
      (cond
        ((and
          (>= key (first node))
          (<= key (second node)))
         (third node))
        ((< key (first node))
         (find-nearest-key (second kd-tree) key))
        ((> key (second node))
         (find-nearest-key (third kd-tree) key))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defclass iterable ()
  ((finished-iterating
    :accessor finished-iterating
    :initform nil))
  (:documentation "This class supports lazy iteration"))

(defgeneric reset-iteration (seq)
  (:documentation "Reset the current and future to the point of root"))

(defmethod reset-iteration :before ((iterable iterable))
  (setf (finished-iterating iterable) nil))

(defgeneric next (seq)
  (:documentation "Set the iterable to the next item"))

(defgeneric root (seq)
  (:documentation "Gratuitous leaching from Clojure's sequence abstraction"))

(defgeneric current  (seq)
  (:documentation "Gratuitous leaching from Clojure's sequence abstraction"))

(defgeneric future (seq)
  (:documentation "Gratuitous leaching from Clojure's sequence abstraction"))

(defgeneric move (seq)
  (:documentation "A stepper function; get data, move to the next element"))

(defmethod move ((iter iterable))
  (let ((data (current iter)))
    (next iter)
    data))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; The table is the entry point for all queries. A query is executed
;;; upon a table, and returns a table.

(defclass table (iterable)
  ((headers
    :initform nil
    :accessor headers
    :initarg :headers)
   (current-iterator
    :initform 0
    :accessor current-iterator
    :documentation "This is the oid (system key row #) that the
    iterator is on")
   (unanalyzed
    :initform t
    :accessor unanalyzed
    :documentation "Has the data been properly analyzed & prepared for
    any sort of analysis doable on static data?")
   (page-key-index
    :initform nil
    :accessor page-key-index
    :documentation "How to get to a given page very quickly")
   (size
    :initform nil
    :accessor size)
   (pages :initform (make-array '(2) :adjustable t :fill-pointer 0)
          :accessor pages))
  (:documentation "A table contains all rows of a given columnar
    structure. This is done via the page mechanism. "))

(defmethod create-index-on-keys ((table table))
  (let ((index
          (k-d-tree
           (loop for page across (pages table)
                 nconc
                 (list (list (first-key page) (last-key page) page))))))
    (setf (page-key-index table) index)))

(defmethod analyze-table ((table table))
  (let ((counter 0))
    (loop for page across (pages table)
          do
             (setf (first-key page) counter)
             (setf (last-key page) (1- (+ counter (lengthg page))))
             (setf counter (+  counter (lengthg page)))))
  (create-index-on-keys table)
  (setf (unanalyzed table) nil))

(defmethod (setf root) (new-value (table table))

  (error "Unable to set table's root"))
(defmethod root ((table table))
  ;; the root of table is actually the first page in pages
  table)

(defmethod reset-iteration ((table table))
  (setf (current-iterator table) 0)
  table)

(defmethod current-page ((table table) index)
  (find-nearest-key (page-key-index table) index))

(defmethod item-at ((table table) (index number))
  (let ((current-page
          (current-page table index)))

    (elt (data current-page)
         (- index (first-key current-page)))))

(defmethod current ((table table))
  (when (unanalyzed table)
    (analyze-table table))

  (when (finished-iterating table)
    (error "Unable to seek data; iterator finished"))

  (let ((data (item-at table (current-iterator table))))
;;      (evict-to-size (cache current-page))
    data))

(defmethod next ((table table))
  (if (> (1- (lengthg table)) (current-iterator table) )
      (incf (current-iterator table))
      (setf (finished-iterating table) t)))


(defmethod lengthg ((table table))
  (loop for page across (pages table)
        sum (lengthg page)))

(defmethod data ((table table))
  "Collects all of the data in the pages and returns it as a single
vector of vectors (suitable for putting into a new table or using as a
new singular page). Probably a bad idea for large data sets."
  (let ((retval (make-array '(2) :adjustable t :fill-pointer 0)))

    (loop for page across (pages table)
          do
             (loop for row across (data page)
                   do (vector-push-extend row retval))
             (evict-to-size *page-cache*))
    retval))

(defmethod insertable-page ((table table))
  (aref (pages table) (1- (length (pages table)))))


(defmethod insert :before ((table table) (anything t) &key)
  (setf (unanalyzed table) t))

(defmethod insert ((table table) (page page) &key)
  (unless (equal-headers (headers page) (headers table))
    (error "Header mismatch: ~a /= ~b" (headers page) (headers table)))

  (vector-push-extend page (pages table))
  table)

(defmethod insert ((table table) (row list) &key)
  (insert table (concatenate 'vector row)))

(defmethod insert ((table table) (row vector) &key)
  (insert (data-frame (insertable-page table)) row)
  table)

;; assumes a 2D list structure
(defmethod bulk-insert ((table table) (rows list))
  (loop for row in rows
        do
        (insert table row)))


(defmethod current ((table table))
  ())

(defmethod current ((seq list))
  (car seq))
(defmethod future ((seq list))
  (cdr seq))

(defmethod next ((seq list))
  (setf (car seq) (cadr seq))
  (setf (cdr seq) (cddr seq)))


(defparameter *foo* nil)
(defun make-dummy-table ()
  (setf *foo*
        (let ((table (make-instance 'table :headers '("0" "1" "2")))
              )

          (loop for i from 0 upto 64 by 2
               do
                  (insert table
                          (make-page (make-instance 'memory-store)
                                     (loop for j from (* i 10000)  below (+ (* i 10000) 10000)
                                           collect `(,j ,(format nil "~a" i) ,(gensym "V"))))))
          table)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Finally, the query.
(defclass query ()
  ())

;;; This is the top layer of access to the cl-linq system
(defgeneric yield (data-frame query &key &allow-other-keys))
(defgeneric update (data-frame query &key &allow-other-keys))
(defgeneric where (data-frame condition &key &allow-other-keys))
(defgeneric group-by (data-frame condition &key &allow-other-keys))
(defgeneric join (data-frame-a data-frame-b &key &allow-other-keys))
(defgeneric inner (data-frame query &key &allow-other-keys))

(defgeneric insert (container object &key &allow-other-keys))

;(defgeneric headers (data-frame &key &allow-other-keys))


;;; The REF api allows a simple generalized reference approach.

(defgeneric ref (data-frame type selector))

(defmethod ref ((data-frame data-frame) (type (eql :row)) selector)
  )
(defmethod ref ((data-frame data-frame) (type (eql :column)) selector))
(defmethod ref ((data-frame data-frame) (type (eql :point)) selector))


(defmethod where ((obj data-frame) condition &key &allow-other-keys)
  (let ((new-data-table (cutout-clone obj)))
    (loop for i from 0 below (lengthg obj)
          do
             (let ((row (array-row-at (data obj) i)))
              (when (funcall condition row)
                (insert new-data-table row :row t))))))
