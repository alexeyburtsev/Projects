function V=rotVM(R)
%V - vector of rotation which is determined with R(matrice of rotation) 
c=1/2*(R(1,1)+R(2,2)+R(3,3)-1);
if c==1
    V=[0;0;0];
elseif c==-1
    phi=pi;
    c1=(R(3,2)-R(2,3));
    c2=(R(1,3)-R(3,1));
    c3=(R(2,1)-R(1,2));
    cc=sqrt(c1^2+c2^2+c3^2);
    c1=c1/cc;
    c2=c2/cc;
    c3=c3/cc;
    V=phi*[c1;c2;c3];
else c^2<1
    s=sqrt(1-c^2);
    phi=asin(s);
    c1=(R(3,2)-R(2,3))/(2*s);
    c2=(R(1,3)-R(3,1))/(2*s);
    c3=(R(2,1)-R(1,2))/(2*s);
    V=phi*[c1;c2;c3];
end