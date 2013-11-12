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

(defclass query ()
  ())

(defclass header ()
  (canonical-name
   canonical-index
   alias-list))

(defclass data-frame ()
  ((headers :reader headers
            :initform nil
            :initarg :headers)
   (width :reader width
          :initform nil
          :initarg :width)
   (lengthg :accessor lengthg
          :initform nil
          :initarg :lengthg)
   (indexes :accessor indexes
            :initform nil
            :initarg :indexes)))

(defun make-data-frame (type data headers)
  (make-instance type
                 :data data
                 :headers headers
                 :width (length headers)))

(defgeneric yield (data-frame query &key &allow-other-keys))
(defgeneric update (data-frame query &key &allow-other-keys))
(defgeneric where (data-frame condition &key &allow-other-keys))
(defgeneric group-by (data-frame condition &key &allow-other-keys))
(defgeneric join (data-frame-a data-frame-b &key &allow-other-keys))
(defgeneric inner (data-frame query &key &allow-other-keys))

(defgeneric insert (data-frame object &key &allow-other-keys))

(defgeneric headers (data-frame &key &allow-other-keys))

(defgeneric ref (data-frame type selector))

(defmethod ref ((data-frame data-frame) (type (eql :row)) selector)
  )
(defmethod ref ((data-frame data-frame) (type (eql :column)) selector))
(defmethod ref ((data-frame data-frame) (type (eql :point)) selector))


(defclass vector-data-frame (data-frame)
  ((data :accessor data
         :initform (make-array (list 0 0) :adjustable t)
         :initarg :data)))

(defun null-clone (object)
  (make-instance 'vector-data-frame
                 :headers (headers object)
                 :width (width object)))

(defmethod insert ((obj vector-data-frame) object &key (row nil))
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
      (incf (lengthg obj)))))

(defgeneric cast-to-vector (source-object)
  (:documentation "Builds a 2D array from source-object"))
(defmethod cast-to-vector ((obj vector-data-frame))
  (values (data obj) (lengthg obj)))
(defmethod cast-to-vector ((list list))
  (let ((length (length list))
        (width (length (car list))))
    (let ((data
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
      (values data length))))
(defmethod cast-to-vector ((array array))
  (values array (array-dimension array 0)))

(defmethod initialize-instance :after ((obj vector-data-frame)  &key)
  (multiple-value-bind (data length)
      (cast-to-vector (data obj))
    (setf (data obj)  data)
    (setf (lengthg obj) length)))

(defmethod where ((obj vector-data-frame) condition &key &allow-other-keys)
  (let ((null-clone obj))
    (loop for i from 0 below (lengthg obj)
          do
             (let ((row (array-row-at (data obj) i)))
              (when (funcall condition row)
                (insert new-data-table row :row t))))))

(defun array-row-at (array subscript)
  (let* ((starting-point
          (array-row-major-index array subscript 0))
         (width (array-dimension array 1))
         (row (make-array (list width))))
    (loop for i from 0 below width
          do
             (setf (aref row i) (row-major-aref array (+ starting-point i))))
    row))
