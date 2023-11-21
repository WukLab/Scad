#ifndef _UTILS_H_
#define _UTILS_H_

#define CHECK(obj, msg) \
    if (obj == NULL) { \
        printf("Object is NULL: " msg "\n"); \
        return 0; \
    }
#define PRINTO(obj) \
    printf("\nDump Python Object: " #obj "\n"); \
    PyObject_Print(obj, stdout, Py_PRINT_RAW);

#ifdef NDEBUG
    #define CHECKO(obj) \
        CHECK(obj, #obj)
#else // NDEBUG
    #define CHECKO(obj) \
        CHECK(obj, #obj) \
        PRINTO(obj)
#endif // NDEBUG

#endif // _UTILS_H_
