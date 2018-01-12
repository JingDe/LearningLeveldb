

#ifndef HISTOGRAM_H
#define HISTOGRAM_H

#include<string>


class Histogram{
public:
	Histogram(){}
	~Histogram(){}
	
	void Clear();
	void Add(double);
	void Merge(const Histogram& other);
	std::string ToString() const;
	
private:
	double min_;//最小时间
	double max_;//最大时间
	double num_;//时间个数
	double sum_;//总时长
	double sum_squares_;//时间平方 的和

	enum{ kNumBuckets=154 };
	static const double kBucketLimit[kNumBuckets]; // 每个数组元素表示一个时间值
	double buckets_[kNumBuckets]; //buckets_[i] 表示落在时间 kBucketLimit[i-1]~kBucketLimit[i]范围内的 时间个数
	
  double Median() const;
  double Percentile(double p) const;
  double Average() const;
  double StandardDeviation() const;
};

#endif