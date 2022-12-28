package cn.itcast.edu.bean

/**
  * 学生题目推荐指数样例类
  */
case class Rating(
                   student_id: Int, //学生id
                   question_id: Int, //题目id
                   rating: Float //推荐指数
                 ) extends Serializable