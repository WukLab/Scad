if [ -z "$1" ]; then
    echo "No query specified"
    exit 1
fi

case "${1}" in 
  1)
    case "${2}" in
      1)
        /usr/bin/time rundisagg "test/src/tpcds_1/step1.o.py" -m "1_out_mem:30441"
        ;;

      2)
        /usr/bin/time rundisagg "test/src/tpcds_1/step2.o.py" -m "2_out_mem:30442"
        ;;

      3)
        /usr/bin/time rundisagg "test/src/tpcds_1/step3.o.py" -m "1_out_mem:30441" "2_out_mem:30442" "3_out_mem:30443"
        ;;

      4)
        /usr/bin/time rundisagg "test/src/tpcds_1/step4.o.py" -m "3_out_mem:30443" "4_out_mem:30444"
        ;;

      5)
        /usr/bin/time rundisagg "test/src/tpcds_1/step5.o.py" -m "5_out_mem:30445"
        ;;

      6)
        /usr/bin/time rundisagg "test/src/tpcds_1/step6.o.py" -m "4_out_mem:30444" "5_out_mem:30445" "6_out_mem:30446"
        ;;

      7)
        /usr/bin/time rundisagg "test/src/tpcds_1/step7.o.py" -m "7_out_mem:30447"
        ;;

      8)
        /usr/bin/time rundisagg "test/src/tpcds_1/step8.o.py" -m "6_out_mem:30446" "7_out_mem:30447" "8_out_mem:30448"
        ;;

      *)
        echo "Unrecongnized/unspecified step"
        exit 1
        ;;
    esac
    ;;
  1-redis)
    case "${2}" in
      1)
        /usr/bin/time rundisagg "test/src/tpcds_1_redis/step1.o.py"
        ;;

      2)
        /usr/bin/time rundisagg "test/src/tpcds_1_redis/step2.o.py"
        ;;

      3)
        /usr/bin/time rundisagg "test/src/tpcds_1_redis/step3.o.py"
        ;;

      4)
        /usr/bin/time rundisagg "test/src/tpcds_1_redis/step4.o.py"
        ;;

      5)
        /usr/bin/time rundisagg "test/src/tpcds_1_redis/step5.o.py"
        ;;

      6)
        /usr/bin/time rundisagg "test/src/tpcds_1_redis/step6.o.py"
        ;;

      7)
        /usr/bin/time rundisagg "test/src/tpcds_1_redis/step7.o.py"
        ;;

      8)
        /usr/bin/time rundisagg "test/src/tpcds_1_redis/step8.o.py"
        ;;

      *)
        echo "Unrecongnized/unspecified step"
        exit 1
        ;;
    esac
    ;;
  16)
    case "${2}" in
      1)
        /usr/bin/time rundisagg "test/src/tpcds_16/step1.o.py" -m "1_out_mem:30441"
        ;;

      2)
        /usr/bin/time rundisagg "test/src/tpcds_16/step2.o.py" -m "2_out_mem:30442"
        ;;

      3)
        /usr/bin/time rundisagg "test/src/tpcds_16/step3.o.py" -m "1_out_mem:30441" "2_out_mem:30442" "3_out_mem:30443"
        ;;

      4)
        /usr/bin/time rundisagg "test/src/tpcds_16/step4.o.py" -m "3_out_mem:30443" "4_out_mem:30444"
        ;;

      5)
        /usr/bin/time rundisagg "test/src/tpcds_16/step5.o.py" -m "3_out_mem:30443" "4_out_mem:30444" "5_out_mem:30445"
        ;;

      6)
        /usr/bin/time rundisagg "test/src/tpcds_16/step6.o.py" -m "5_out_mem:30445" "6_out_mem:30446"
        ;;

      *)
        echo "Unrecongnized/unspecified step"
        exit 1
        ;;
    esac
    ;;
  16-redis)
    case "${2}" in
      1)
        /usr/bin/time rundisagg "test/src/tpcds_16_redis/step1.o.py"
        ;;

      2)
        /usr/bin/time rundisagg "test/src/tpcds_16_redis/step2.o.py"
        ;;

      3)
        /usr/bin/time rundisagg "test/src/tpcds_16_redis/step3.o.py"
        ;;

      4)
        /usr/bin/time rundisagg "test/src/tpcds_16_redis/step4.o.py"
        ;;

      5)
        /usr/bin/time rundisagg "test/src/tpcds_16_redis/step5.o.py"
        ;;

      6)
        /usr/bin/time rundisagg "test/src/tpcds_16_redis/step6.o.py"
        ;;

      *)
        echo "Unrecongnized/unspecified step"
        exit 1
        ;;
    esac
    ;;
  94)
    case "${2}" in
      1)
        /usr/bin/time rundisagg "test/src/tpcds_94/step1.o.py" -m "1_out_mem:30441"
        ;;

      2)
        /usr/bin/time rundisagg "test/src/tpcds_94/step2.o.py" -m "2_out_mem:30442"
        ;;

      3)
        /usr/bin/time rundisagg "test/src/tpcds_94/step3.o.py" -m "1_out_mem:30441" "2_out_mem:30442" "3_out_mem:30443"
        ;;

      4)
        /usr/bin/time rundisagg "test/src/tpcds_94/step4.o.py" -m "3_out_mem:30443" "4_out_mem:30444"
        ;;

      5)
        /usr/bin/time rundisagg "test/src/tpcds_94/step5.o.py" -m "3_out_mem:30443" "4_out_mem:30444" "5_out_mem:30445"
        ;;

      6)
        /usr/bin/time rundisagg "test/src/tpcds_94/step6.o.py" -m "5_out_mem:30445" "6_out_mem:30446"
        ;;

      *)
        echo "Unrecongnized/unspecified step"
        exit 1
        ;;
    esac
    ;;
  94-redis)
    case "${2}" in
      1)
        /usr/bin/time rundisagg "test/src/tpcds_94_redis/step1.o.py"
        ;;

      2)
        /usr/bin/time rundisagg "test/src/tpcds_94_redis/step2.o.py"
        ;;

      3)
        /usr/bin/time rundisagg "test/src/tpcds_94_redis/step3.o.py"
        ;;

      4)
        /usr/bin/time rundisagg "test/src/tpcds_94_redis/step4.o.py"
        ;;

      5)
        /usr/bin/time rundisagg "test/src/tpcds_94_redis/step5.o.py"
        ;;

      6)
        /usr/bin/time rundisagg "test/src/tpcds_94_redis/step6.o.py"
        ;;

      *)
        echo "Unrecongnized/unspecified step"
        exit 1
        ;;
    esac
    ;;

  95)
    case "${2}" in
      1)
        /usr/bin/time rundisagg "test/src/tpcds_95/step1.o.py" -m "1_out_mem:30441"
        ;;

      2)
        /usr/bin/time rundisagg "test/src/tpcds_95/step2.o.py" -m "1_out_mem:30441" "2_out_mem:30442"
        ;;

      3)
        /usr/bin/time rundisagg "test/src/tpcds_95/step3.o.py" -m "3_out_mem:30443"
        ;;

      4)
        /usr/bin/time rundisagg "test/src/tpcds_95/step4.o.py" -m "4_out_mem:30444"
        ;;

      5)
        /usr/bin/time rundisagg "test/src/tpcds_95/step5.o.py" -m "2_out_mem:30442" "3_out_mem:30443" "4_out_mem:30444" "5_out_mem:30445"
        ;;

      6)
        /usr/bin/time rundisagg "test/src/tpcds_95/step6.o.py" -m "6_out_mem:30446"
        ;;

      7)
        /usr/bin/time rundisagg "test/src/tpcds_95/step7.o.py" -m "5_out_mem:30445" "6_out_mem:30446" "7_out_mem:30447"
        ;;

      8)
        /usr/bin/time rundisagg "test/src/tpcds_95/step8.o.py" -m "7_out_mem:30447" "8_out_mem:30448"
        ;;
      *)
        echo "Unrecongnized/unspecified step"
        exit 1
        ;;
    esac
    ;;
  95-redis)
    case "${2}" in
      1)
        /usr/bin/time rundisagg "test/src/tpcds_95_redis/step1.o.py"
        ;;

      2)
        /usr/bin/time rundisagg "test/src/tpcds_95_redis/step2.o.py"
        ;;

      3)
        /usr/bin/time rundisagg "test/src/tpcds_95_redis/step3.o.py"
        ;;

      4)
        /usr/bin/time rundisagg "test/src/tpcds_95_redis/step4.o.py"
        ;;

      5)
        /usr/bin/time rundisagg "test/src/tpcds_95_redis/step5.o.py"
        ;;

      6)
        /usr/bin/time rundisagg "test/src/tpcds_95_redis/step6.o.py"
        ;;

      7)
        /usr/bin/time rundisagg "test/src/tpcds_95_redis/step7.o.py"
        ;;

      8)
        /usr/bin/time rundisagg "test/src/tpcds_95_redis/step8.o.py"
        ;;

      *)
        echo "Unrecongnized/unspecified step"
        exit 1
        ;;
    esac
    ;;
  *)
    echo "Unrecongnized query"
    exit 1
    ;;
esac
