;;;; -*- Mode: LISP; Syntax: ANSI-Common-Lisp; Base: 10 -*-
;;;; *************************************************************************
;;;; FILE IDENTIFICATION
;;;;
;;;; Name:     postgresql-socket-api.lisp
;;;; Purpose:  Low-level PostgreSQL interface using sockets
;;;; Authors:  Kevin M. Rosenberg based on original code by Pierre R. Mai
;;;; Created:  Feb 2002
;;;;
;;;; $Id$
;;;;
;;;; This file, part of CLSQL, is Copyright (c) 2002-2004 by Kevin M. Rosenberg
;;;; and Copyright (c) 1999-2001 by Pierre R. Mai
;;;;
;;;; CLSQL users are granted the rights to distribute and use this software
;;;; as governed by the terms of the Lisp Lesser GNU Public License
;;;; (http://opensource.franz.com/preamble.html), also known as the LLGPL.
;;;; *************************************************************************

(in-package #:postgresql-socket3)

(defmethod clsql-sys:database-type-load-foreign ((database-type (eql :postgresql-socket3)))
  t)

(defmethod clsql-sys:database-type-library-loaded ((database-type
                                          (eql :postgresql-socket)))
  "T if foreign library was able to be loaded successfully. Always true for
socket interface"
  t)