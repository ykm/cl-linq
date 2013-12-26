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
    :initform (make-array (list 0 0) :adjustable t)
    :initarg :data
    :documentation "A remark on the data format. The `data-frame` is
    designed to be used as part of a data system with indexes applied
    across the system. Since an efficiently accessed data format
    relies on the sort method (whether in a vector or by a tree), and
    this is a *general* system, the design decision was made to keep
    data quickly accessible once the index was known - hence, the
    array format. The current implementation uses 2D arrays. Another
    mechanism might well be to use an array of arrays.")
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
  (:documentation "Builds a 2D array from source-object and returns
  the data and the length thereof "))

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
              (loop for i from 0 below (second (array-dimensions (data obj)))
                    collect i)))
    obj))

(defmethod cast-to-vector ((obj data-frame))
  (values (data obj) (lengthg obj)))

(defmethod cast-to-vector ((list list))
  "This presumes that `list` is two dimensions."
  (let* ((length (length list))
         (width (length (car list)))
         (data
           (make-array
            (list length width)
            :adjustable t)))
    (loop
      for row in list
      for i from 0
      do
         (loop
           for column in row
           for j from 0
           do
              (setf (aref data i j)
                    column)))
    (values data length)))

(defmethod cast-to-vector ((array array))
  (values array (array-dimension array 0)))

(defun array-row-at (array subscript)
  (let* ((starting-point
          (array-row-major-index array subscript 0))
         (width (array-dimension array 1))
         (row (make-array (list width))))
    (loop for i from 0 below width
          do
             (setf (aref row i) (row-major-aref array (+ starting-point i))))
    row))

(defmethod cutout-clone ((frame data-frame))
  "A \"cutout\" clone of the data frame, with the same headers and
  width, but without the data."
  (make-instance 'data-frame
                 :headers (headers frame)
                 :width (width frame)))

(defmethod insert ((obj data-frame) object &key (row nil))
  (when row
    (let ((dim (array-dimensions (data obj))))
      (incf (first dim))
      (adjust-array (data obj) dim :initial-element nil)
      (loop for i from 0 below (second dim)
            do
            (setf (aref (data obj)
                        (1- (first dim))
                        i)
                  (aref object i)))
      (incf (data-count obj)))))

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
  ((first-key
    :accessor first-key
    :initform nil
    :initarg :first-key)
   (last-key
    :accessor last-key
    :initform nil
    :initarg :last-key)
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

(defmethod data :before ((page page))
  "Automatically load up from the store if it isn't already there."
  (with-slots (data)
      page
    (unless data
      (load-page page))))

;; Ensures that data being inserted is a data frame.
(defmethod (setf data) :around (new-data (obj page))
  (if (and new-data
           (not (typep new-data 'data-frame)))
      (call-next-method (make-data-frame new-data) obj)
      (call-next-method)))

(defmethod lengthg ((obj page))
  (lengthg (data obj)))

(defmethod unload-page  ((page page))
  (with-slots (data) page
    (setf data nil)))

(defmethod load-page ((page page))
  (setf (data page)
        (data (backing-store page))))

(defmethod flush-page ((obj page))
  (setf (data (backing-store obj)) obj))

(defun make-page (backing-store &optional data (headers nil))
  (let ((obj (make-instance 'caching-page :backing-store backing-store)))
    (when data
      (setf (data obj)
            (make-data-frame data headers)))
    obj))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; The caching logic manages loading and unloading in smart ways.
(defclass cache ()
  ((size
    :accessor size
    :initform 0)
   (members
    :accessor members
    :initform (cl-containers:make-container 'cl-containers:heap-container
                                            :sorter #'>
                                            :key #'evict-selector)
    :initarg :members
    :documentation "Elements should conform to cachable-item")
   (test
       :accessor test
     :initform #'eql
     :initarg :test)
   (cache-overflow-p
    :accessor cache-overflow-p
    :initform #'(lambda (self) (> (size self) 5))
    :initarg :caching-page-overflow-p)))


(defmethod evict-worst ((cache cache))
  (let ((old-item (cl-containers:delete-first (members cache))))
    (format t "~&Evicting ~a~%" old-item)
    (unload-item old-item))
  (decf (size cache)))

(defmethod evict-all ((cache cache))
  (when (funcall (cache-overflow-p cache) cache)
    ;; Resort the container
    (let ((known-elements (cl-containers:collect-elements (members cache))))
      (cl-containers:empty! (members cache))
      (loop for element in known-elements
            do (cl-containers:insert-item (members cache) element)))

    ;; Strip out the worst
    (loop while (funcall (cache-overflow-p cache) cache)
          do
             (evict-worst cache))
    t))


;;; This is what can be put in a cache
(defclass cachable-item ()
  ((access-count
    :accessor access-count
    :initform 0
    :documentation "Increments each time that DATA is called on this
    page.")))


(defmethod (setf access-count) :after (new-value (item cachable-item))
  (format t "~&AC being touched~%"))

(defmethod evict-selector ((item cachable-item))
  (access-count item))

(defmethod data :after ((item cachable-item))
  (incf (access-count item)))

(defgeneric unload-item (cachable-item)
  (:documentation "Unloads the internal backing data from memory"))

(defgeneric flush-item (cachable-item)
  (:documentation "A cachable item should occasionally flush to the
  backing store in case it gets evicted"))

(defmethod add-to-cache ((cache cache) (item cachable-item))
  (cl-containers:insert-item (members cache)
                             item)
  (incf (size cache)))

(defmethod remove-from-cache ((cache cache) (item cachable-item))
  ;; This may already have been removed.
  (when (cl-containers:delete-item (members cache) item)
    (decf (size cache)))

  (when (data item)
    (warn "Inconsistency: page and cache went out of sync; data
    should have been unloaded before the cache was unloaded" )))

;;;;;;;;;;

(defparameter *page-cache*
  (make-instance 'cache
                 :test #'eql))
(defparameter *foo* nil)
(defun make-dummy-pages ()
  (setf *foo*
        (loop for i from 0 upto 20 by 2
              collect
              (make-page (make-instance 'memory-store)
                         `((,i :a :b) (,(1+ i) :a c))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; The page of the caching.
(defclass caching-page (page cachable-item)
  ())

(defmethod initialize-instance :after ((page caching-page) &key)
  (add-to-cache *page-cache* page))

(defmethod unload-item ((page caching-page))
  (unload-page page))

(defmethod unload-page :after ((page caching-page))
  (remove-from-cache *page-cache* page))

(defmethod load-page :after ((obj caching-page))
  (add-to-cache *page-cache* obj))

(defmethod flush-cached-pages ((cache cache))
  "Flushes the currently cached pages out to the backing store."
  (loop for page in (cl-containers:collect-elements *page-cache*)
        do (flush-page page)))


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
;;; The table is the entry point for all queries. A query is executed
;;; upon a table, and returns a table.

(defclass table ()
  ((headers :initform nil)
   (lengthg :initform nil)
   page)

  (:documentation "A table contains all rows of a given columnar
    structure. This is done via the page mechanism. "))

(defclass query ()
  ())

;;; This is the top layer of access to the cl-linq system
(defgeneric yield (data-frame query &key &allow-other-keys))
(defgeneric update (data-frame query &key &allow-other-keys))
(defgeneric where (data-frame condition &key &allow-other-keys))
(defgeneric group-by (data-frame condition &key &allow-other-keys))
(defgeneric join (data-frame-a data-frame-b &key &allow-other-keys))
(defgeneric inner (data-frame query &key &allow-other-keys))

(defgeneric insert (data-frame object &key &allow-other-keys))

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
