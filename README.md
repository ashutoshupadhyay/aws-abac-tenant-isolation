# aws-abac-tenant-isolation

**Implement tenant isolation for Amazon S3 and Aurora PostgreSQL by using ABAC**

In software as a service (SaaS) systems, which are designed to be used by multiple customers, isolating tenant data is a fundamental responsibility for SaaS providers. The practice of isolation of data in a multi-tenant application platform is called tenant isolation. In this post, we describe an approach you can use to achieve tenant isolation in Amazon S3 and Amazon Aurora PostgreSQL-Compatible Edition databases by implementing attribute-based access control (ABAC). You can also adapt the same approach to achieve tenant isolation in other AWS services.

ABAC in Amazon Web Services (AWS), which uses tags to store attributes, offers advantages over the traditional role-based access control (RBAC) model. You can use fewer permissions policies, update your access control more efficiently as you grow, and last but not least, apply granular permissions for various AWS services. These granular permissions help you to implement an effective and coherent tenant isolation strategy for your customers and clients. Using the ABAC model helps you scale your permissions and simplify the management of granular policies. The ABAC model reduces the time and effort it takes to maintain policies that allow access to only the required resources.

The solution we present here uses the pool model of data partitioning. The pool model helps you avoid the higher costs of duplicated resources for each tenant and the specialized infrastructure code required to set up and maintain those copies.

**Solution overview**

![image](https://user-images.githubusercontent.com/2657469/120420817-ade6e380-c397-11eb-8014-0ca1cdcc7608.png)

