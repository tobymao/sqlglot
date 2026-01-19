#include <Python.h>


typedef struct {
  long *x;
  long *y;
} Obj;

static PyObject *test(PyObject *self, PyObject *args) {
  PyObject *obj;

  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }

  long num = PyLong_AsLong(obj);

  for (long i = 0; i <= 10000000; i++) {
    num += i;
    Obj *myobj = malloc(sizeof(Obj));
    myobj->x = &num;
    myobj->y = &num;
    free(myobj);
  }



  //for (int i = 0; i <= 10000000; i++) {
  //  PyObject *a = PyLong_FromLong(i);
  //  obj = PyNumber_Add(a, obj);
  //  Py_DECREF(a);
  //}

  return PyLong_FromLong(num);
};

static PyMethodDef cql_methods[] = {
  {"test", test, METH_VARARGS, "print hello world"},
  {NULL, NULL, 0, NULL}
};

static struct PyModuleDef cql_module = {
  .m_base = PyModuleDef_HEAD_INIT,
  .m_name = "cql",
  .m_size = 0,
  .m_methods = cql_methods,
};

PyMODINIT_FUNC PyInit_cql(void) {
  return PyModuleDef_Init(&cql_module);
}
