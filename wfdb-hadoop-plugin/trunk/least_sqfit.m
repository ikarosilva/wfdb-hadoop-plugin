function slope=least_sqfit(x,y)
  %Find the knee point by fitting to lines, one at the begennign and one at the 
  % the, and check where they intercept.  Then get slope from begining to that intercept point


N=length(x);
mid=round(N/2);

%First fit
[a1,b1]=fit(x(1:mid),y(1:mid));

%Second fit
[a2,b2]=fit(x(mid:end),y(mid:end));

%Get knee point, fr MSE, integers should be good enough
  knee=round((a2-a1)/(b1-b2));

%Get slope from beggining to knee,  if its in the region
  if( knee > 1 && knee <N )
    [slope,b]=fit(x(1:knee),y(1:knee));
  else
    %if there is no knee, use the entire series
    [slope,b]=fit(x,y);
    end


    function [a,b]=fit(x,y)

      n=length(x);
mx=mean(x);
my=mean(y);
ss=sum(x.^2);
sxy=sum(x.*y);


a= ( sxy-n*(mx*my) ) / (ss - ( ( sum(x).^2 )/n )  );
b=my - a*mx;
