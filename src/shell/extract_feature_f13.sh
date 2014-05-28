#! /bin/bash

awk 'BEGIN{
	FS="\t";
}
{
	id=$1;
	if( !(id in idlist) )
	{
		if ( NR == FNR )
		{
			idlist[id] = 1;
			numq[id]=0; sum[id]=0; qwords[id]=0; allwords[id]=0; 
			url[id]=0; user[id]=0; rt[id]=0; hashtag[id]=0;
			qurl[id]=0; quser[id]=0; qrt[id]=0; qhashtag[id]=0;
		}
		else
			next;
	}
	str=tolower($3);
	split(str,words," ");
	m1 = match(str,"is (that|this|it) true");
	m2 = match(str,"wha[a]*t[\\?!][\\?!]*");
	m3 = match(str,"(rumor|debunk|unconfirmed)");
	m4 = match(str,"(that|this|it) is not true");
	m5 = match(str,"(real|really)\\?") ;
	str2=tolower($3);
	t1=0; t2=0; t3=0; t4=0;
	while( sub("http","",str2) ) t1++;
	while( sub("@[a-z0-9_]*","",str2)) t2++;
	while( sub("rt","",str2)) t3++;
	while( sub("#[a-z0-9_]*", "", str2)) t4++;
	if( m1+m2+m3+m4+m5 )
	{
		numq[id]++;
		for( i in words )
		{
			count[words[i],id]++;qcount[words[i],id]++;
			allwords[id]++;qwords[id]++;
		}
		qurl[id]+=t1; quser[id]+=t2; qrt[id]+=t3; qhashtag[id]+=t4;
	}
	else
	{
		for( i in words )
		{
			count[words[i],id]++;allwords[id]++;
		}
	}
	url[id]+=t1; user[id]+=t2; rt[id]+=t3; hashtag[id]+=t4;
	sum[id]++;
}
END{
	for( comb in count )
	{
		split(comb,sep,SUBSEP);
		if( comb in qcount )
			entq[sep[2]]+=qcount[comb]*log(qcount[comb]);
		ent[sep[2]]+=count[comb]*log(count[comb]);
	}
	for( id in idlist )
	{
		if( qwords[id] == 0 )
			entq[id] = 0;
		else
			entq[id] = log(qwords[id])-entq[id]/qwords[id];
		if( allwords[id] == 0 )
			ent[id] = 0;
		else
			ent[id] = log(allwords[id])-ent[id]/allwords[id];
		f1 = sum[id]; f2 = numq[id]; f3 = ent[id]; f4 = entq[id];
		if( sum[id] == 0)
		{
			f5 = 0;
			f7 = 0;
			f10=0;f11=0;f12=0;f13=0;
		}
		else
		{
			f5 = allwords[id]/sum[id];
			f7 = numq[id]/sum[id];
			f10=url[id]/sum[id]; f11=user[id]/sum[id]; f12=rt[id]/sum[id];f13=hashtag[id]/sum[id];
		}
		if( numq[id] == 0)
		{
			f6 = 0;
			f9 = 0;
			f14=0; f15=0; f16=0; f17=0;
		}
		else
		{
			f6 = qwords[id]/numq[id];
			f9 = qwords[id]*sum[id]/(allwords[id]*numq[id]);
			f14 =qurl[id]/numq[id]; f15=quser[id]/numq[id]; f16=qrt[id]/numq[id]; f17=qhashtag[id]/numq[id];
		}
		if ( ent[id] == 0 || entq[id] == 0)
			f8 = 0;
		else
			f8 = entq[id]/ent[id];
		print id"\t"numq[id]"\t"sum[id]"\t1:"f5" 2:"f6" 3:"f7" 4:"f8" 5:"f9" 6:"f10" 7:"f11" 8:"f12" 9:"f13" 10:"f14" 11:"f15" 12:"f16" 13:"f17;
	}
}' output_tweets matched*
