
void FilterBlockBuilder::AddKey(const Slice& key)
{
	Slice k=key;
	start_.push_back(keys_.size());
	keys_.append(k.data(), k.size());
}