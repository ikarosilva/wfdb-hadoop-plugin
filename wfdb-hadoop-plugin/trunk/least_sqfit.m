function alpha=least_sqfit(fileName)
  %Find the knee point by fitting to lines, one at the begennign and one at the 
  % the, and check where they intercept.  Then get slope from begining to that intercept point


data=load(fileName);
%Each pair of columns is a series, first one is the original
M=length(data(1,:));
stats=zeros(M/2,1)+NaN;
ind=1;
for m=1:2:M 
x=data(:,m);
y=data(:,m+1);
N=length(x);
mid=round(N/2);
alpha=0;
%First fit
[a1,b1]=fit(x(1:mid),y(1:mid));

%Second fit
[a2,b2]=fit(x(mid:end),y(mid:end));

%Get knee point, fr MSE, integers should be good enough
  knee=round((a2-a1)/(b1-b2));

%Get slope from beggining to knee,  if its in the region
  slope=NaN;  
if( knee > 1 && knee <N )
    [slope,b]=fit(x(1:knee),y(1:knee));
  else
    %if there is no knee, use the entire series
    [slope,b]=fit(x,y);
    end
    stats(ind)=slope;
ind=ind+1;
end

%Get range of surrogate
mn=quantile(stats(2:end),0.05);
mx=quantile(stats(2:end),0.95);

%Origingal data is relevant if outside the range
if(stats(1) > mx || stats(1) < mn)
  alpha=stats(1);
 else
   alpha=0;
end






%%%Helper function

    function [a,b]=fit(x,y)

      n=length(x);
mx=mean(x);
my=mean(y);
ss=sum(x.^2);
sxy=sum(x.*y);


a= ( sxy-n*(mx*my) ) / (ss - ( ( sum(x).^2 )/n )  );
b=my - a*mx;
