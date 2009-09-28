;;;; -*- Mode: LISP; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;; *************************************************************************
;;;; FILE IDENTIFICATION
;;;;
;;;; Name:     postgresql-socket-sql.sql
;;;; Purpose:  High-level PostgreSQL interface using socket
;;;; Authors:  Kevin M. Rosenberg based on original code by Pierre R. Mai
;;;; Created:  Feb 2002
;;;;
;;;; $Id$
;;;;
;;;; This file, part of CLSQL, is Copyright (c) 2002-2007 by Kevin M. Rosenberg
;;;; and Copyright (c) 1999-2001 by Pierre R. Mai
;;;;
;;;; CLSQL users are granted the rights to distribute and use this software
;;;; as governed by the terms of the Lisp Lesser GNU Public License
;;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;; *************************************************************************

(in-package #:cl-user)

(defpackage :clsql-postgresql-socket3
    (:use #:common-lisp #:clsql-sys #:postgresql-socket)
    (:export #:postgresql-socket-database)
    (:documentation "This is the CLSQL socket interface to PostgreSQL."))

(in-package #:clsql-postgresql-socket3)

;; interface foreign library loading routines

(clsql-sys:database-type-load-foreign :postgresql-socket3)


(defmethod database-initialize-database-type ((database-type
                                               (eql :postgresql-socket3)))
  t)


;; Field type conversion
(defun convert-to-clsql-warning (database condition)
  (ecase *backend-warning-behavior*
    (:warn
     (warn 'sql-database-warning :database database
           :message (postgresql-condition-message condition)))
    (:error
     (error 'sql-database-error :database database
            :message (format nil "Warning upgraded to error: ~A"
                             (postgresql-condition-message condition))))
    ((:ignore nil)
     ;; do nothing
     )))

(defun convert-to-clsql-error (database expression condition)
  (error 'sql-database-data-error
         :database database
         :expression expression
         :error-id (type-of condition)
         :message (postgresql-condition-message condition)))

(defmacro with-postgresql-handlers
    ((database &optional expression)
     &body body)
  (let ((database-var (gensym))
        (expression-var (gensym)))
    `(let ((,database-var ,database)
           (,expression-var ,expression))
       (handler-bind ((postgresql-warning
                       (lambda (c)
                         (convert-to-clsql-warning ,database-var c)))
                      (postgresql-error
                       (lambda (c)
                         (convert-to-clsql-error
                          ,database-var ,expression-var c))))
         ,@body))))



(defclass postgresql-socket3-database (generic-postgresql-database)
  ((connection :accessor database-connection :initarg :connection
               :type cl-postgres:database-connection)))

(defmethod database-type ((database postgresql-socket3-database))
  :postgresql-socket3)

(defmethod database-name-from-spec (connection-spec (database-type (eql :postgresql-socket3)))
  (check-connection-spec connection-spec database-type
                         (host db user password &optional port options tty))
  (destructuring-bind (host db user password &optional port options tty)
      connection-spec
    (declare (ignore password options tty))
    (concatenate 'string
      (etypecase host
        (null
         "localhost")
        (pathname (namestring host))
        (string host))
      (when port
        (concatenate 'string
                     ":"
                     (etypecase port
                       (integer (write-to-string port))
                       (string port))))
      "/" db "/" user)))

(defmethod database-connect (connection-spec
                             (database-type (eql :postgresql-socket)))
  (check-connection-spec connection-spec database-type
                         (host db user password &optional port options tty))
  (destructuring-bind (host db user password &optional
                            (port +postgresql-server-default-port+)
                            (options "") (tty ""))
      connection-spec
    (handler-case
        (handler-bind ((warning
                        (lambda (c)
                          (warn 'sql-warning
                                :format-control "~A"
                                :format-arguments
                                (list (princ-to-string c))))))
          (cl-postgres:open-database
	   :database db
	   :user user
	   :password password
	   :host host
	   :port port
           ))
      (cl-postgres:database-error (c)
        ;; Connect failed
        (error 'sql-connection-error
               :database-type database-type
               :connection-spec connection-spec
               :error-id (type-of c)
               :message (postgresql-condition-message c)))
      (:no-error (connection)
                 ;; Success, make instance
                 (make-instance 'postgresql-socket3-database
                                :name (database-name-from-spec connection-spec database-type)
                                :database-type :postgresql-socket3
                                :connection-spec connection-spec
                                :connection connection)))))

(defmethod database-disconnect ((database postgresql-socket3-database))
  (cl-postgres:close-database (database-connection database))
  t)

(defvar *include-field-names* nil)

(cl-postgres:def-row-reader clsql-default-row-reader (fields)
  (values (loop :while (next-row)
		:collect (loop :for field :across fields
			       :collect (next-field field)))
	  (when *include-field-names*
	    (loop :for field :across fields
		  :collect (field-name field)))))

(defmethod database-query ((expression string) (database postgresql-socket3-database) result-types field-names)
  (let ((connection (database-connection database)))
    (with-postgresql-handlers (database expression)
      (let ((*include-field-names* field-names))
	(cl-postgres:exec-query connection expression #'clsql-default-row-reader))
      )))

(defmethod database-execute-command
    ((expression string) (database postgresql-socket3-database))
  (let ((connection (database-connection database)))
    (with-postgresql-handlers (database expression)
      (exec-query connection expression))))

;;;; Cursoring interface

(defclass cursor ()
  ((next-row :accessor next-row :initarg :next-row :initform nil)
   (fields :accessor fields :initarg :fields :initform nil)
   (next-field :accessor next-field :initarg :next-field :initform nil)
   (done :accessor done :initarg :done :initform nil)))

(defvar *cursor* ())

(cl-postgres:def-row-reader clsql-cursored-row-reader (fields)
  (setf *cursor*
	(make-instance 'cursor :next-row #'next-row :fields fields :next-field #'next-field)))

(defmethod database-query-result-set ((expression string)
                                      (database postgresql-socket3-database)
                                      &key full-set result-types)
  (declare (ignore full-set))
  (let ((connection (database-connection database))
	*cursor*)
    (with-postgresql-handlers (database expression)
      (cl-postgres:exec-query connection expression 'clsql-cursored-row-reader)
      (values *cursor* (length (fields *cursor*))))))

(defmethod database-dump-result-set (result-set
                                     (database postgresql-socket-database))
  (unless (done result-set)
    (loop :while (funcall (next-row result-set))))
  T)

(defmethod database-store-next-row (result-set
                                    (database postgresql-socket-database)
                                    list)
  (when (and (not (done result-set))
	     (setf (done result-set) (funcall (next-row result-set))))
    
    (let* ((data (loop :for field :across (fields result-set)
		       :collect (funcall (next-field result-set) field))))
      ;; Maybe?
      (setf (car list) (car data)
	    (cdr list) (cdr data)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;


(defmethod database-create (connection-spec (type (eql :postgresql-socket3)))
  (destructuring-bind (host name user password &optional port options tty) connection-spec
    (let ((database (database-connect (list host "postgres" user password)
                                      type)))
      (setf (slot-value database 'clsql-sys::state) :open)
      (unwind-protect
           (database-execute-command (format nil "create database ~A" name) database)
        (database-disconnect database)))))

(defmethod database-destroy (connection-spec (type (eql :postgresql-socket3)))
  (destructuring-bind (host name user password &optional port optional tty) connection-spec
    (let ((database (database-connect (list host "postgres" user password)
                                      type)))
      (setf (slot-value database 'clsql-sys::state) :open)
      (unwind-protect
          (database-execute-command (format nil "drop database ~A" name) database)
        (database-disconnect database)))))


(defmethod database-probe (connection-spec (type (eql :postgresql-socket3)))
  (when (find (second connection-spec) (database-list connection-spec type)
              :test #'string-equal)
    t))


;; Database capabilities

(defmethod db-backend-has-create/destroy-db? ((db-type (eql :postgresql-socket3)))
  nil)

(defmethod db-type-has-fancy-math? ((db-type (eql :postgresql-socket3)))
  t)

(defmethod db-type-default-case ((db-type (eql :postgresql-socket3)))
  :lower)

(defmethod database-underlying-type ((database postgresql-socket3-database))
  :postgresql)

(when (clsql-sys:database-type-library-loaded :postgresql-socket3)
  (clsql-sys:initialize-database-type :database-type :postgresql-socket3))
